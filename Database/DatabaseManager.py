import os
import logging
import asyncio
from typing import Dict, List, Any, Callable
from datetime import datetime, timedelta

import pytz
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import UpdateOne
from dotenv import load_dotenv

from Database.database.collection_config import CollectionConfig
from Database.database.collection_manager import CollectionManager
from Database.database.connection_pool import ConnectionPool
from Database.database.database_properties import DatabaseProperties
from Database.database.define_collections import DefineCollections

# Load environment variables
load_dotenv()
primary = os.getenv("IMPERIALCODEX")
secondary = os.getenv("EMPIRESDATA")
third = os.getenv("THIRD")
logger = logging.getLogger("DatabaseManager")


class DatabaseManager(DefineCollections, DatabaseProperties):
    """
    Comprehensive MongoDB database manager with connection pooling,
    CRUD operations, caching, error handling, and performance optimization.
    """

    def __init__(self, primary_uri: str = None, secondary_uri: str = None, third_uri: str = None, **additional_uris):
        self.primary_uri = primary_uri or primary
        self.secondary_uri = secondary_uri or secondary
        self.third_uri = third_uri or third
        self.additional_uris = additional_uris

        if not self.primary_uri:
            raise ValueError("Primary MongoDB URI not provided")

        # Create connection pools for each URI
        self.connection_pools = {}
        self.connection_pools['primary'] = ConnectionPool(self.primary_uri, connection_name='primary')

        if self.secondary_uri:
            self.connection_pools['secondary'] = ConnectionPool(self.secondary_uri, connection_name='secondary')
            logger.info("Secondary connection pool configured")

        if self.third_uri:
            self.connection_pools['third'] = ConnectionPool(self.third_uri, connection_name='third')
            logger.info("Third connection pool configured")
        else:
            logger.warning("No Third MongoDB URI provided")

        # Add additional connections (third, fourth, fifth, etc.)
        connection_names = ['fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth']

        for i, name in enumerate(connection_names, 3):
            uri_key = f"{name}_uri"
            if uri_key in self.additional_uris:
                self.connection_pools[name] = ConnectionPool(self.additional_uris[uri_key], connection_name=name)
                logger.info(f"{name.capitalize()} connection pool configured")
            else:
                # Stop at first missing connection to maintain order
                break

        self.databases: Dict[str, AsyncIOMotorDatabase] = {}
        self.collections: Dict[str, CollectionManager] = {}
        self._collection_configs: Dict[str, CollectionConfig] = {}
        self._initialized = False
        self._lock = asyncio.Lock()

        # Define collection configurations with indexes
        self._define_collection_configs()

    async def initialize(self):
        """Initialize the database manager with connection pooling and collection setup."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            try:
                logger.info("Initializing DatabaseManager...")

                # Initialize all connection pools
                for name, pool in self.connection_pools.items():
                    await pool.initialize()
                    logger.info(f"Initialized {name} connection pool")

                # Dynamically discover and initialize databases from all connections
                for connection_name, pool in self.connection_pools.items():
                    try:
                        client = await pool.get_client()

                        # Get list of database names (excluding system databases)
                        db_names = await client.list_database_names()
                        non_system_dbs = [db for db in db_names if db not in ['admin', 'local', 'config']]

                        logger.info(
                            f"Found {len(non_system_dbs)} databases in {connection_name} connection: {non_system_dbs}")

                        # Initialize databases for this connection
                        for db_name in non_system_dbs:
                            # Use connection name as prefix if it's not primary to avoid conflicts
                            db_key = db_name
                            if connection_name != 'primary' and db_name in self.databases:
                                # If database name conflicts with primary, use connection prefix
                                db_key = f"{connection_name}_{db_name}"
                                logger.debug(
                                    f"Database name conflict: {db_name} exists in multiple connections. Using {db_key}")

                            self.databases[db_key] = client[db_name]
                            logger.debug(f"Initialized database '{db_key}' from {connection_name} connection")

                    except Exception as e:
                        logger.warning(f"Error discovering databases from {connection_name} connection: {e}")
                        continue

                # Initialize collections with managers
                await self._initialize_collections()

                # Create indexes
                await self._create_all_indexes()

                self._initialized = True
                logger.info(f"DatabaseManager initialized successfully with {len(self.databases)} databases")

            except Exception as e:
                logger.error(f"Failed to initialize DatabaseManager: {e}")
                raise

    async def _initialize_collections(self):
        """Initialize collection managers."""
        for config_key, config in self._collection_configs.items():
            try:
                # Get the appropriate client based on the config's connection
                connection_name = config.connection
                if connection_name not in self.connection_pools:
                    logger.warning(
                        f"Connection '{connection_name}' not available for {config_key}, falling back to primary")
                    connection_name = 'primary'

                client = await self.connection_pools[connection_name].get_client()
                database = client[config.database]
                collection = database[config.name]

                # Create capped collection if specified
                if config.capped:
                    try:
                        await database.create_collection(
                            config.name,
                            capped=True,
                            size=config.max_size,
                            max=config.max_documents
                        )
                    except Exception:
                        # Collection might already exist
                        pass

                manager = CollectionManager(collection, config)
                self.collections[config_key] = manager

                logger.debug(f"Initialized collection manager for {config_key} on {connection_name} connection")

            except Exception as e:
                logger.error(f"Error initializing collection {config_key}: {e}")
                raise

    async def _create_all_indexes(self):
        """Create indexes for all collections."""
        for config_key, manager in self.collections.items():
            try:
                await manager.create_indexes()
            except Exception as e:
                logger.warning(f"Error creating indexes for {config_key}: {e}")

    def _ensure_initialized(self):
        """Ensure the database manager is initialized."""
        if not self._initialized:
            raise RuntimeError("DatabaseManager not initialized. Call initialize() first.")

    # Collection Access Methods

    def get_database(self, name: str) -> AsyncIOMotorDatabase:
        """
        Get a database by name.

        Args:
            name: Database name

        Returns:
            Database instance
        """
        self._ensure_initialized()
        if name not in self.databases:
            # Try to find which connection has this database
            for connection_name, pool in self.connection_pools.items():
                try:
                    client = pool.client
                    if name in client.list_database_names():
                        self.databases[name] = client[name]
                        return self.databases[name]
                except Exception:
                    continue
            raise ValueError(f"Database '{name}' not found in any connection")
        return self.databases[name]

    def get_collection_manager(self, collection_key: str) -> CollectionManager:
        """
        Get a collection manager by key.

        Args:
            collection_key: Collection configuration key

        Returns:
            CollectionManager instance
        """
        self._ensure_initialized()
        if collection_key not in self.collections:
            raise ValueError(f"Collection '{collection_key}' not configured")
        return self.collections[collection_key]

    def get_raw_collection(self, database_name: str, collection_name: str) -> AsyncIOMotorCollection:
        """
        Get raw collection access for advanced operations.

        Args:
            database_name: Database name
            collection_name: Collection name

        Returns:
            Raw collection instance
        """
        database = self.get_database(database_name)
        return database[collection_name]

    def get_client(self, connection_name: str = 'primary') -> AsyncIOMotorClient:
        """
        Get a client for a specific connection.

        Args:
            connection_name: Name of the connection pool ('primary', 'secondary', 'third', etc.)

        Returns:
            AsyncIOMotorClient instance
        """
        self._ensure_initialized()
        if connection_name not in self.connection_pools:
            raise ValueError(f"Connection '{connection_name}' not configured")
        return self.connection_pools[connection_name].client

    async def get_client_async(self, connection_name: str = 'primary') -> AsyncIOMotorClient:
        """
        Get a client for a specific connection asynchronously.

        Args:
            connection_name: Name of the connection pool ('primary', 'secondary', 'third', etc.)

        Returns:
            AsyncIOMotorClient instance
        """
        self._ensure_initialized()
        if connection_name not in self.connection_pools:
            raise ValueError(f"Connection '{connection_name}' not configured")
        return await self.connection_pools[connection_name].get_client()

    def get_database_from_connection(self, database_name: str,
                                     connection_name: str = 'primary') -> AsyncIOMotorDatabase:
        """
        Get a database from a specific connection.

        Args:
            database_name: Database name
            connection_name: Connection pool name

        Returns:
            Database instance from specified connection
        """
        self._ensure_initialized()
        if connection_name not in self.connection_pools:
            raise ValueError(f"Connection '{connection_name}' not configured")

        client = self.connection_pools[connection_name].client
        return client[database_name]

    def get_raw_collection_from_connection(self, database_name: str, collection_name: str,
                                           connection_name: str = 'primary') -> AsyncIOMotorCollection:
        """
        Get raw collection access from a specific connection for advanced operations.

        Args:
            database_name: Database name
            collection_name: Collection name
            connection_name: Connection pool name

        Returns:
            Raw collection instance from specified connection
        """
        database = self.get_database_from_connection(database_name, connection_name)
        return database[collection_name]

    # Connection Management Methods

    def get_connection_names(self) -> List[str]:
        """Get list of all available connection names."""
        return list(self.connection_pools.keys())

    def has_connection(self, connection_name: str) -> bool:
        """Check if a specific connection is available."""
        return connection_name in self.connection_pools

    async def add_connection(self, connection_name: str, uri: str):
        """Dynamically add a new connection."""
        if connection_name in self.connection_pools:
            raise ValueError(f"Connection '{connection_name}' already exists")

        self.connection_pools[connection_name] = ConnectionPool(uri, connection_name=connection_name)
        await self.connection_pools[connection_name].initialize()
        logger.info(f"Added new connection: {connection_name}")

    async def remove_connection(self, connection_name: str):
        """Remove a connection (cannot remove primary)."""
        if connection_name == 'primary':
            raise ValueError("Cannot remove primary connection")

        if connection_name in self.connection_pools:
            await self.connection_pools[connection_name].close()
            del self.connection_pools[connection_name]
            logger.info(f"Removed connection: {connection_name}")

    # Transaction Support

    async def start_session(self, connection_name: str = 'primary', **kwargs):
        """Start a new database session for transactions on specified connection."""
        self._ensure_initialized()
        client = await self.get_client_async(connection_name)
        return await client.start_session(**kwargs)

    async def with_transaction(self, callback: Callable, session_options: Dict = None,
                               connection_name: str = 'primary'):
        """
        Execute a callback within a transaction on specified connection.

        Args:
            callback: Async function to execute within transaction
            session_options: Options for the session
            connection_name: Connection pool name

        Returns:
            Result of the callback function
        """
        session_options = session_options or {}

        async with await self.start_session(connection_name, **session_options) as session:
            async with session.start_transaction():
                return await callback(session)

    # Utility Methods

    async def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        self._ensure_initialized()

        stats = {
            'databases': {},
            'total_collections': 0,
            'total_documents': 0,
            'total_size': 0,
            'connections': {}
        }

        try:
            # Get stats for each connection
            for connection_name, pool in self.connection_pools.items():
                try:
                    client = await pool.get_client()
                    connection_stats = await client.admin.command('serverStatus')
                    stats['connections'][connection_name] = {
                        'ok': connection_stats.get('ok', 0),
                        'host': connection_stats.get('host', 'unknown'),
                        'version': connection_stats.get('version', 'unknown')
                    }
                except Exception as e:
                    stats['connections'][connection_name] = f"error: {e}"

            # Get database stats (using primary connection for existing logic)
            client = await self.connection_pools['primary'].get_client()

            for db_name, database in self.databases.items():
                db_stats = await database.command('dbStats')
                collection_stats = {}

                for collection_key, manager in self.collections.items():
                    if manager.config.database == db_name:
                        coll_stats = await manager.get_stats()
                        collection_stats[manager.name] = coll_stats
                        stats['total_documents'] += coll_stats.get('count', 0)
                        stats['total_size'] += coll_stats.get('size', 0)

                stats['databases'][db_name] = {
                    'collections': db_stats.get('collections', 0),
                    'dataSize': db_stats.get('dataSize', 0),
                    'indexSize': db_stats.get('indexSize', 0),
                    'collection_details': collection_stats
                }

                stats['total_collections'] += db_stats.get('collections', 0)

            return stats

        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return stats

    async def cleanup_old_data(self, days_to_keep: int = 90):
        """
        Clean up old data across collections based on created_at field.

        Args:
            days_to_keep: Number of days of data to retain
        """
        cutoff_date = datetime.now(tz=pytz.UTC) - timedelta(days=days_to_keep)
        cleanup_results = {}

        # Collections that should have old data cleaned up
        cleanup_collections = [
            'serverdata_analytics',
            'serverdata_events'
        ]

        for collection_key in cleanup_collections:
            if collection_key in self.collections:
                try:
                    manager = self.collections[collection_key]
                    deleted_count = await manager.delete_many({
                        'created_at': {'$lt': cutoff_date}
                    })
                    cleanup_results[collection_key] = deleted_count
                    logger.info(f"Cleaned up {deleted_count} old records from {collection_key}")
                except Exception as e:
                    logger.error(f"Error cleaning up {collection_key}: {e}")
                    cleanup_results[collection_key] = f"Error: {e}"

        return cleanup_results

    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check on all connections."""
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now(tz=pytz.UTC).isoformat(),
            'connections': {},
            'databases': {},
            'collections': {}
        }

        try:
            # Check all connection pools
            for name, pool in self.connection_pools.items():
                try:
                    client = await pool.get_client()
                    await client.admin.command('ping')
                    health_status['connections'][name] = 'healthy'
                except Exception as e:
                    health_status['connections'][name] = f'error: {e}'
                    health_status['status'] = 'degraded'

            # Check each database (using primary connection for existing databases)
            for db_name in self.databases.keys():
                try:
                    db = self.get_database(db_name)
                    await db.command('ping')
                    health_status['databases'][db_name] = 'healthy'
                except Exception as e:
                    health_status['databases'][db_name] = f'error: {e}'
                    health_status['status'] = 'degraded'

            # Check collection managers
            for collection_key, manager in self.collections.items():
                try:
                    await manager.count_documents({})
                    health_status['collections'][collection_key] = 'healthy'
                except Exception as e:
                    health_status['collections'][collection_key] = f'error: {e}'
                    health_status['status'] = 'degraded'

        except Exception as e:
            health_status['status'] = 'unhealthy'
            health_status['error'] = str(e)

        return health_status

    async def close(self):
        """Close all database connections and cleanup resources."""
        try:
            logger.info("Closing DatabaseManager...")

            # Clear collections and databases
            self.collections.clear()
            self.databases.clear()

            # Close all connection pools
            for name, pool in self.connection_pools.items():
                await pool.close()
                logger.info(f"Closed {name} connection pool")

            self.connection_pools.clear()
            self._initialized = False
            logger.info("DatabaseManager closed successfully")

        except Exception as e:
            logger.error(f"Error closing DatabaseManager: {e}")


# Global database manager instance
db_manager = DatabaseManager()


# Utility functions for common database patterns

async def ensure_unique_constraint(manager: CollectionManager,
                                   field: str,
                                   value: Any,
                                   exclude_id: Any = None) -> bool:
    """
    Ensure a field value is unique in the collection.

    Args:
        manager: Collection manager
        field: Field name to check
        value: Value that should be unique
        exclude_id: Document ID to exclude from uniqueness check

    Returns:
        True if value is unique, False otherwise
    """
    filter_dict = {field: value}
    if exclude_id:
        filter_dict['_id'] = {'$ne': exclude_id}

    existing = await manager.find_one(filter_dict)
    return existing is None


async def paginate_results(manager: CollectionManager,
                           filter_dict: Dict[str, Any] = None,
                           sort: List[tuple] = None,
                           page_size: int = 50,
                           page: int = 1) -> Dict[str, Any]:
    """
    Paginate query results.

    Args:
        manager: Collection manager
        filter_dict: Query filter
        sort: Sort specification
        page_size: Number of items per page
        page: Page number (1-based)

    Returns:
        Dictionary with pagination info and results
    """
    filter_dict = filter_dict or {}
    skip = (page - 1) * page_size

    # Get total count and results concurrently
    total_count, results = await asyncio.gather(
        manager.count_documents(filter_dict),
        manager.find_many(filter_dict, sort=sort, limit=page_size, skip=skip)
    )

    total_pages = (total_count + page_size - 1) // page_size

    return {
        'results': results,
        'pagination': {
            'current_page': page,
            'page_size': page_size,
            'total_items': total_count,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    }


async def batch_upsert(manager: CollectionManager,
                       documents: List[Dict[str, Any]],
                       match_fields: List[str]) -> Dict[str, int]:
    """
    Perform batch upsert operations based on matching fields.

    Args:
        manager: Collection manager
        documents: List of documents to upsert
        match_fields: Fields to match for upsert decision

    Returns:
        Dictionary with counts of inserted and updated documents
    """
    if not documents or not match_fields:
        return {'inserted': 0, 'updated': 0}

    operations = []
    now = datetime.now(tz=pytz.UTC)

    for doc in documents:
        # Build filter from match fields
        filter_dict = {field: doc[field] for field in match_fields if field in doc}

        # Prepare update document
        update_doc = doc.copy()
        update_doc['updated_at'] = now
        if 'created_at' not in update_doc:
            update_doc['created_at'] = now

        operation = UpdateOne(
            filter_dict,
            {'$set': update_doc},
            upsert=True
        )
        operations.append(operation)

    result = await manager.bulk_write(operations, ordered=False)

    return {
        'inserted': result['inserted_count'] + result['upserted_count'],
        'updated': result['modified_count']
    }