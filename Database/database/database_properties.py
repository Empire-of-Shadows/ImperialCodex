from Database.database.collection_manager import CollectionManager


class DatabaseProperties:
    @property
    def daily_wyr_mappings(self) -> CollectionManager:
        """Get Daily WYR Mappings collection manager."""
        return self.get_collection_manager('daily_wyr_mappings')

    @property
    def suggestions_suggestions(self) -> CollectionManager:
        """Get Suggestions collection manager."""
        return self.get_collection_manager('suggestions_suggestions')

    @property
    def daily_wyr(self) -> CollectionManager:
        """Get Daily WYR collection manager."""
        return self.get_collection_manager('daily_wyr')

    @property
    def serverdata_roles(self) -> CollectionManager:
        """Get ServerData Roles collection manager."""
        return self.get_collection_manager('serverdata_roles')

    @property
    def serverdata_channels(self) -> CollectionManager:
        """Get ServerData Channels collection manager."""
        return self.get_collection_manager('serverdata_channels')

    @property
    def serverdata_members(self) -> CollectionManager:
        """Get ServerData Members collection manager."""
        return self.get_collection_manager('serverdata_members')

    @property
    def serverdata_guilds(self) -> CollectionManager:
        """Get ServerData Guilds collection manager."""
        return self.get_collection_manager('serverdata_guilds')

    @property
    def daily_wyr_leaderboard(self) -> CollectionManager:
        """Get Daily WYR Leaderboard collection manager."""
        return self.get_collection_manager('daily_wyr_leaderboard')

    @property
    def suggestions_votes(self) -> CollectionManager:
        """Get Suggestions Votes collection manager."""
        return self.get_collection_manager('suggestions_votes')

    @property
    def user_stats(self) -> CollectionManager:
        """Get User Stats collection manager."""
        return self.get_collection_manager('ecom_users')