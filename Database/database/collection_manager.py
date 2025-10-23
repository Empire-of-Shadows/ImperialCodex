import time
from datetime import datetime
from functools import wraps
from typing import Dict, Any, List, Optional, Union

import backoff
import pytz
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne, InsertOne, DeleteOne, ReplaceOne
from pymongo.errors import BulkWriteError, ConnectionFailure, OperationFailure

from Database.database.collection_config import CollectionConfig
from utils.logger import get_logger

logger = get_logger("CollectionManager")

def with_retry(max_retries: int = 3, backoff_factor: float = 1.0):
    """Decorator for database operations with exponential backoff retry."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            @backoff.on_exception(
                backoff.expo,
                (ConnectionFailure, OperationFailure),
                max_tries=max_retries,
                factor=backoff_factor,
                jitter=backoff.random_jitter
            )
            async def _execute():
                return await func(*args, **kwargs)

            return await _execute()

        return wrapper

    return decorator

class CollectionManager:
    """Manages CRUD operations for a specific collection with caching and optimization."""

    def __init__(self, collection: AsyncIOMotorCollection, config: CollectionConfig):
        self.collection = collection
        self.config = config
        self.name = config.name
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._default_cache_duration = 300  # 5 minutes

    # CREATE Operations

    @with_retry(max_retries=3)
    async def create_one(self, document: Dict[str, Any], **kwargs) -> Any:
        """
        Insert a single document.

        Args:
            document: The document to insert
            **kwargs: Additional options for insert_one

        Returns:
            The inserted document's ID
        """
        try:
            document['created_at'] = datetime.now(tz=pytz.UTC)
            document['updated_at'] = datetime.now(tz=pytz.UTC)

            result = await self.collection.insert_one(document, **kwargs)
            logger.debug(f"Inserted document with ID {result.inserted_id} into {self.name}")

            # Invalidate relevant caches
            self._invalidate_cache()

            return result.inserted_id
        except Exception as e:
            logger.error(f"Error creating document in {self.name}: {e}")
            raise

    @with_retry(max_retries=3)
    async def create_many(self, documents: List[Dict[str, Any]],
                          ordered: bool = False, **kwargs) -> List[Any]:
        """
        Insert multiple documents with bulk operations.

        Args:
            documents: List of documents to insert
            ordered: Whether to perform ordered inserts
            **kwargs: Additional options for insert_many

        Returns:
            List of inserted document IDs
        """
        if not documents:
            return []

        try:
            # Add timestamps to all documents
            now = datetime.now(tz=pytz.UTC)
            for doc in documents:
                doc['created_at'] = now
                doc['updated_at'] = now

            result = await self.collection.insert_many(documents, ordered=ordered, **kwargs)
            logger.debug(f"Inserted {len(result.inserted_ids)} documents into {self.name}")

            # Invalidate relevant caches
            self._invalidate_cache()

            return result.inserted_ids
        except BulkWriteError as bwe:
            logger.error(f"Bulk write error in {self.name}: {bwe.details}")
            # Return successfully inserted IDs even on partial failure
            return [oid for oid in bwe.details.get('insertedIds', {}).values()]
        except Exception as e:
            logger.error(f"Error creating documents in {self.name}: {e}")
            raise

    # READ Operations

    @with_retry(max_retries=2)
    async def find_one(self, filter_dict: Dict[str, Any] = None,
                       projection: Dict[str, Any] = None,
                       cache_key: str = None,
                       cache_duration: int = None,
                       **kwargs) -> Optional[Dict[str, Any]]:
        """
        Find a single document with optional caching.

        Args:
            filter_dict: Query filter
            projection: Fields to include/exclude
            cache_key: Key for caching the result
            cache_duration: Cache duration in seconds
            **kwargs: Additional options for find_one

        Returns:
            The found document or None
        """
        # Check cache first
        if cache_key and self._is_cached(cache_key):
            logger.debug(f"Cache hit for {cache_key} in {self.name}")
            return self._get_cached(cache_key)

        try:
            filter_dict = filter_dict or {}
            result = await self.collection.find_one(filter_dict, projection, **kwargs)

            # Cache the result if cache_key is provided
            if cache_key and result:
                duration = cache_duration or self._default_cache_duration
                self._set_cache(cache_key, result, duration)

            return result
        except Exception as e:
            logger.error(f"Error finding document in {self.name}: {e}")
            raise

    @with_retry(max_retries=2)
    async def find_many(self, filter_dict: Dict[str, Any] = None,
                        projection: Dict[str, Any] = None,
                        sort: List[tuple] = None,
                        limit: int = None,
                        skip: int = 0,
                        **kwargs) -> List[Dict[str, Any]]:
        """
        Find multiple documents with cursor optimization.

        Args:
            filter_dict: Query filter
            projection: Fields to include/exclude
            sort: Sort specification
            limit: Maximum number of documents
            skip: Number of documents to skip
            **kwargs: Additional options for find

        Returns:
            List of found documents
        """
        try:
            filter_dict = filter_dict or {}
            cursor = self.collection.find(filter_dict, projection, **kwargs)

            if sort:
                cursor = cursor.sort(sort)
            if skip > 0:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)

            documents = await cursor.to_list(length=limit)
            logger.debug(f"Found {len(documents)} documents in {self.name}")

            return documents
        except Exception as e:
            logger.error(f"Error finding documents in {self.name}: {e}")
            raise

    @with_retry(max_retries=2)
    async def count_documents(self, filter_dict: Dict[str, Any] = None, **kwargs) -> int:
        """
        Count documents matching the filter.

        Args:
            filter_dict: Query filter
            **kwargs: Additional options for count_documents

        Returns:
            Number of matching documents
        """
        try:
            filter_dict = filter_dict or {}
            count = await self.collection.count_documents(filter_dict, **kwargs)
            return count
        except Exception as e:
            logger.error(f"Error counting documents in {self.name}: {e}")
            raise

    @with_retry(max_retries=2)
    async def aggregate(self, pipeline: List[Dict[str, Any]], **kwargs) -> List[Dict[str, Any]]:
        """
        Perform aggregation pipeline operations.

        Args:
            pipeline: Aggregation pipeline
            **kwargs: Additional options for aggregate

        Returns:
            List of aggregation results
        """
        try:
            cursor = self.collection.aggregate(pipeline, **kwargs)
            results = await cursor.to_list(length=None)
            logger.debug(f"Aggregation returned {len(results)} results from {self.name}")
            return results
        except Exception as e:
            logger.error(f"Error in aggregation for {self.name}: {e}")
            raise

    # UPDATE Operations

    @with_retry(max_retries=3)
    async def update_one(self, filter_dict: Dict[str, Any],
                         update_dict: Dict[str, Any],
                         upsert: bool = False,
                         **kwargs) -> bool:
        """
        Update a single document.

        Args:
            filter_dict: Query filter
            update_dict: Update operations
            upsert: Whether to insert if no document matches
            **kwargs: Additional options for update_one

        Returns:
            True if document was modified, False otherwise
        """
        try:
            # Add updated_at timestamp
            if '$set' not in update_dict:
                update_dict['$set'] = {}
            update_dict['$set']['updated_at'] = datetime.now(tz=pytz.UTC)

            result = await self.collection.update_one(filter_dict, update_dict,
                                                      upsert=upsert, **kwargs)

            success = result.modified_count > 0 or (upsert and result.upserted_id is not None)
            if success:
                logger.debug(f"Updated document in {self.name}")
                self._invalidate_cache()

            return success
        except Exception as e:
            logger.error(f"Error updating document in {self.name}: {e}")
            raise

    @with_retry(max_retries=3)
    async def update_many(self, filter_dict: Dict[str, Any],
                          update_dict: Dict[str, Any],
                          **kwargs) -> int:
        """
        Update multiple documents.

        Args:
            filter_dict: Query filter
            update_dict: Update operations
            **kwargs: Additional options for update_many

        Returns:
            Number of documents modified
        """
        try:
            # Add updated_at timestamp
            if '$set' not in update_dict:
                update_dict['$set'] = {}
            update_dict['$set']['updated_at'] = datetime.now(tz=pytz.UTC)

            result = await self.collection.update_many(filter_dict, update_dict, **kwargs)

            if result.modified_count > 0:
                logger.debug(f"Updated {result.modified_count} documents in {self.name}")
                self._invalidate_cache()

            return result.modified_count
        except Exception as e:
            logger.error(f"Error updating documents in {self.name}: {e}")
            raise

    @with_retry(max_retries=3)
    async def replace_one(self, filter_dict: Dict[str, Any],
                          replacement: Dict[str, Any],
                          upsert: bool = False,
                          **kwargs) -> bool:
        """
        Replace a single document.

        Args:
            filter_dict: Query filter
            replacement: Replacement document
            upsert: Whether to insert if no document matches
            **kwargs: Additional options for replace_one

        Returns:
            True if document was replaced, False otherwise
        """
        try:
            # Add timestamps to replacement
            replacement['updated_at'] = datetime.now(tz=pytz.UTC)
            if 'created_at' not in replacement:
                replacement['created_at'] = datetime.now(tz=pytz.UTC)

            result = await self.collection.replace_one(filter_dict, replacement,
                                                       upsert=upsert, **kwargs)

            success = result.modified_count > 0 or (upsert and result.upserted_id is not None)
            if success:
                logger.debug(f"Replaced document in {self.name}")
                self._invalidate_cache()

            return success
        except Exception as e:
            logger.error(f"Error replacing document in {self.name}: {e}")
            raise

    # DELETE Operations

    @with_retry(max_retries=3)
    async def delete_one(self, filter_dict: Dict[str, Any], **kwargs) -> bool:
        """
        Delete a single document.

        Args:
            filter_dict: Query filter
            **kwargs: Additional options for delete_one

        Returns:
            True if document was deleted, False otherwise
        """
        try:
            result = await self.collection.delete_one(filter_dict, **kwargs)

            if result.deleted_count > 0:
                logger.debug(f"Deleted document from {self.name}")
                self._invalidate_cache()
                return True

            return False
        except Exception as e:
            logger.error(f"Error deleting document from {self.name}: {e}")
            raise

    @with_retry(max_retries=3)
    async def delete_many(self, filter_dict: Dict[str, Any], **kwargs) -> int:
        """
        Delete multiple documents.

        Args:
            filter_dict: Query filter
            **kwargs: Additional options for delete_many

        Returns:
            Number of documents deleted
        """
        try:
            result = await self.collection.delete_many(filter_dict, **kwargs)

            if result.deleted_count > 0:
                logger.debug(f"Deleted {result.deleted_count} documents from {self.name}")
                self._invalidate_cache()

            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting documents from {self.name}: {e}")
            raise

    # BULK Operations

    @with_retry(max_retries=3)
    async def bulk_write(self, operations: List[Union[UpdateOne, InsertOne, DeleteOne, ReplaceOne]],
                         ordered: bool = False, **kwargs) -> Dict[str, Any]:
        """
        Perform bulk write operations for maximum efficiency.

        Args:
            operations: List of bulk operations
            ordered: Whether to perform operations in order
            **kwargs: Additional options for bulk_write

        Returns:
            Dictionary with operation results
        """
        if not operations:
            return {'inserted_count': 0, 'modified_count': 0, 'deleted_count': 0}

        try:
            # Add timestamps to operations where applicable
            now = datetime.now(tz=pytz.UTC)
            for op in operations:
                if isinstance(op, (UpdateOne, ReplaceOne)):
                    if hasattr(op, '_update') and isinstance(op._update, dict):
                        if '$set' not in op._update:
                            op._update['$set'] = {}
                        op._update['$set']['updated_at'] = now
                elif isinstance(op, InsertOne):
                    if hasattr(op, '_doc') and isinstance(op._doc, dict):
                        op._doc['created_at'] = now
                        op._doc['updated_at'] = now

            result = await self.collection.bulk_write(operations, ordered=ordered, **kwargs)

            logger.debug(f"Bulk operation completed on {self.name}: "
                         f"inserted={result.inserted_count}, "
                         f"modified={result.modified_count}, "
                         f"deleted={result.deleted_count}")

            # Invalidate cache if any modifications occurred
            if result.inserted_count > 0 or result.modified_count > 0 or result.deleted_count > 0:
                self._invalidate_cache()

            return {
                'inserted_count': result.inserted_count,
                'modified_count': result.modified_count,
                'deleted_count': result.deleted_count,
                'upserted_count': result.upserted_count,
                'upserted_ids': result.upserted_ids
            }
        except BulkWriteError as bwe:
            logger.warning(f"Bulk write error in {self.name}: {bwe.details}")
            # Return partial results
            result = bwe.details
            return {
                'inserted_count': result.get('nInserted', 0),
                'modified_count': result.get('nModified', 0),
                'deleted_count': result.get('nRemoved', 0),
                'upserted_count': result.get('nUpserted', 0),
                'errors': result.get('writeErrors', [])
            }
        except Exception as e:
            logger.error(f"Error in bulk write for {self.name}: {e}")
            raise

    # UTILITY Methods

    async def create_indexes(self) -> List[str]:
        """Create indexes defined in the collection configuration."""
        if not self.config.indexes:
            return []

        try:
            index_names = await self.collection.create_indexes(self.config.indexes)
            logger.info(f"Created {len(index_names)} indexes for {self.name}: {index_names}")
            return index_names
        except Exception as e:
            logger.error(f"Error creating indexes for {self.name}: {e}")
            raise

    async def drop_indexes(self, index_names: List[str] = None):
        """Drop specified indexes or all non-default indexes."""
        try:
            if index_names:
                for index_name in index_names:
                    await self.collection.drop_index(index_name)
                logger.info(f"Dropped indexes {index_names} from {self.name}")
            else:
                await self.collection.drop_indexes()
                logger.info(f"Dropped all indexes from {self.name}")
        except Exception as e:
            logger.error(f"Error dropping indexes from {self.name}: {e}")
            raise

    async def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        try:
            stats = await self.collection.database.command('collStats', self.name)
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avgObjSize': stats.get('avgObjSize', 0),
                'storageSize': stats.get('storageSize', 0),
                'indexes': stats.get('nindexes', 0),
                'totalIndexSize': stats.get('totalIndexSize', 0)
            }
        except Exception as e:
            logger.warning(f"Error getting stats for {self.name}: {e}")
            return {}

    # CACHE Management

    def _is_cached(self, key: str) -> bool:
        """Check if a key is cached and not expired."""
        if key not in self._cache:
            return False

        if key in self._cache_ttl and time.time() > self._cache_ttl[key]:
            del self._cache[key]
            del self._cache_ttl[key]
            return False

        return True

    def _get_cached(self, key: str) -> Any:
        """Get a cached value."""
        return self._cache.get(key)

    def _set_cache(self, key: str, value: Any, duration: int):
        """Set a cached value with TTL."""
        self._cache[key] = value
        self._cache_ttl[key] = time.time() + duration

    def _invalidate_cache(self, pattern: str = None):
        """Invalidate cache entries, optionally matching a pattern."""
        if pattern:
            keys_to_remove = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_remove:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)
        else:
            self._cache.clear()
            self._cache_ttl.clear()
