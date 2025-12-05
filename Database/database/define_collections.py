from pymongo import IndexModel

from Database.database.collection_config import CollectionConfig
from utils.logger import get_logger

logger = get_logger("DefineCollections")

class DefineCollections:
    def _define_collection_configs(self):
        """Define collection configurations including indexes for optimal performance."""

        # Daily collections
        self._collection_configs['daily_wyr'] = CollectionConfig(
            name='WYR',
            database='Daily',
            connection='primary',
            indexes=[
                IndexModel([('date', -1)]),
                IndexModel([('guild_id', 1), ('date', -1)]),
                IndexModel([('created_at', -1)])
            ]
        )

        self._collection_configs['daily_wyr_leaderboard'] = CollectionConfig(
            name='WYR_Leaderboard',
            database='Daily',
            connection='primary',
            indexes=[
                IndexModel([('user_id', 1), ('guild_id', 1)]),
                IndexModel([('score', -1)]),
                IndexModel([('updated_at', -1)])
            ]
        )

        self._collection_configs['daily_wyr_mappings'] = CollectionConfig(
            name='WYR_Mappings',
            database='Daily',
            connection='primary',
            indexes=[
                IndexModel([('guild_id', 1)]),
                IndexModel([('created_at', -1)]),
            ]
        )

        # Guide collections
        self._collection_configs['guide_menues'] = CollectionConfig(
            name='Menus',
            database='Guide',
            connection='primary',
            indexes=[
                IndexModel([('user_id', 1), ('guild_id', 1)]),
                IndexModel([('score', -1)]),
                IndexModel([('updated_at', -1)])
            ]
        )

        # Prime Drops Collections
        self._collection_configs['prime_drops'] = CollectionConfig(
            name='AmazonPrime',
            database='PrimeDrops',
            connection='primary',
            indexes=[
                IndexModel([('uid', 1)], unique=True, name='uid_unique'),
                IndexModel([('short_href', 1)], name='short_href_lookup'),
                IndexModel([('label', 'text'), ('description', 'text')], name='text_search')
            ]
        )

        # Profile collections
        self._collection_configs['profilecard_themes'] = CollectionConfig(
            name='CustomThemes',
            database='ProfileCard',
            connection='primary',
            indexes=[
                IndexModel([('user_id', 1), ('guild_id', 1)], name='user_guild'),
                IndexModel([('user_id', 1), ('guild_id', 1), ('theme_name', 1)], unique=True,
                           name='user_guild_theme_unique')
            ]
        )

        self._collection_configs['profilecard_preferences'] = CollectionConfig(
            name='ProfilePreferences',
            database='ProfileCard',
            connection='primary',
            indexes=[
                IndexModel([('user_id', 1), ('guild_id', 1)], unique=True, name='user_guild_unique')
            ]
        )

        # Suggestions collections
        self._collection_configs['suggestions_suggestions'] = CollectionConfig(
            name='Suggestions',
            database='Suggestions',
            connection='primary',
            indexes=[
                IndexModel([('guild_id', 1), ('status', 1)]),
                IndexModel([('author_id', 1)]),
                IndexModel([('created_at', -1)]),
                IndexModel([('guild_id', 1), ('suggestion_id', 1)], unique=True)
            ]
        )

        self._collection_configs['suggestions_votes'] = CollectionConfig(
            name='Votes',
            database='Suggestions',
            connection='primary',
            indexes=[
                IndexModel([('suggestion_id', 1), ('user_id', 1)], unique=True),
                IndexModel([('suggestion_id', 1)]),
                IndexModel([('user_id', 1), ('created_at', -1)])
            ]
        )

        self._collection_configs['suggestions_templates'] = CollectionConfig(
            name='Templates',
            database='Suggestions',
            connection='primary',
            indexes=[
                # Todo - Add indexes for Templates
            ]
        )

        self._collection_configs['suggestions_userstats'] = CollectionConfig(
            name='UserStats',
            database='Suggestions',
            connection='primary',
            indexes=[
                IndexModel([('user_id', 1)], unique=True, name='user_id_unique'),
                IndexModel([('last_activity', -1)], name='last_activity_desc')
            ]
        )

        self._collection_configs['suggestions_notification_queue'] = CollectionConfig(
            name='NotificationQueue',
            database='Suggestions',
            connection='primary',
            indexes=[
                IndexModel(
                    [('sent', 1), ('created_at', 1)],
                    name='pending_by_created_at',
                    partialFilterExpression={'sent': False}
                ),
                IndexModel(
                    [('user_id', 1), ('suggestion_id', 1), ('type', 1)],
                    name='unique_pending_per_user_suggestion_type',
                    unique=True,
                    partialFilterExpression={'sent': False}
                ),
                IndexModel([('user_id', 1), ('suggestion_id', 1)], name='user_suggestion_lookup')
            ]
        )

        # Updates and Drops collections
        self._collection_configs['updates_monthly'] = CollectionConfig(
            name='StatsMonthly',
            database='Updates-Drops',
            connection='primary',
            indexes=[
                IndexModel([('_id.coll', 1), ('_id.year', -1), ('_id.month', -1)], name='by_coll_year_month_desc'),
                IndexModel([('updated_at', -1)], name='updated_at_desc')
            ]
        )

        self._collection_configs['updates_totals'] = CollectionConfig(
            name='StatsTotals',
            database='Updates-Drops',
            connection='primary',
            indexes=[
                IndexModel([('updated_at', -1)], name='updated_at_desc')
            ]
        )
        logger.info("Database Manager initialized\n\n\n\n\n\n\n")

        # Boost Tracking collections
        self._collection_configs['serverdata_boosts'] = CollectionConfig(
            name='Boosts',
            database='Server-Data',
            connection='secondary',
            indexes=[
                IndexModel([('user_id', 1)]),
                IndexModel([('guild_id', 1)]),
                IndexModel([('is_active', 1)]),
                IndexModel([('boost_start', -1)]),
                IndexModel([('guild_id', 1), ('user_id', 1)], unique=True),
                IndexModel([('guild_id', 1), ('is_active', 1), ('boost_start', -1)])
            ]
        )

        self._collection_configs['serverdata_boost_events'] = CollectionConfig(
            name='Boost_Events',
            database='Server-Data',
            connection='secondary',
            indexes=[
                IndexModel([('guild_id', 1), ('timestamp', -1)]),
                IndexModel([('user_id', 1), ('timestamp', -1)]),
                IndexModel([('event_type', 1)]),
                IndexModel([('timestamp', -1)])
            ]
        )

        self._collection_configs['serverdata_channels'] = CollectionConfig(
            name='Channels',
            database='Server-Data',
            connection='secondary',
            indexes=[
                IndexModel([('guild_id', 1), ('id', 1)], unique=True),
                IndexModel([('guild_id', 1), ('type', 1)]),
                IndexModel([('guild_id', 1), ('category_id', 1)])
            ]
        )

        # ServerData collections
        self._collection_configs['serverdata_guilds'] = CollectionConfig(
            name='Guilds',
            database='Server-Data',
            connection='secondary',
            indexes=[
                IndexModel([('id', 1)], unique=True),
                IndexModel([('owner_id', 1)]),
                IndexModel([('member_count', -1)]),
                IndexModel([('updated_at', -1)])
            ]
        )

        self._collection_configs['serverdata_members'] = CollectionConfig(
            name='Members',
            database='Server-Data',
            connection='secondary',
            indexes=[
                IndexModel([('guild_id', 1), ('id', 1)], unique=True),
                IndexModel([('guild_id', 1), ('bot', 1)]),
                IndexModel([('guild_id', 1), ('joined_at', -1)]),
                IndexModel([('guild_id', 1), ('roles', 1)])
            ]
        )

        self._collection_configs['serverdata_roles'] = CollectionConfig(
            name='Roles',
            database='Server-Data',
            connection='secondary',
            indexes=[
            ]
        )

        self._collection_configs['ecom_users'] = CollectionConfig(
            name='Stats',
            database='Users',
            connection='third',
            indexes=[
                # Primary user identification - unique compound index
                IndexModel([('user_id', 1), ('guild_id', 1)], unique=True),

                # Guild-based queries with sorting
                IndexModel([('guild_id', 1), ('xp', -1)]),
                IndexModel([('guild_id', 1), ('level', -1)]),
                IndexModel([('guild_id', 1), ('embers', -1)]),
                IndexModel([('guild_id', 1), ('updated_at', -1)]),

                # Message stats leaderboards
                IndexModel([('guild_id', 1), ('message_stats.messages', -1)]),
                IndexModel([('guild_id', 1), ('message_stats.daily_streak', -1)]),

                # Voice stats leaderboards
                IndexModel([('guild_id', 1), ('voice_stats.voice_seconds', -1)]),
                IndexModel([('guild_id', 1), ('voice_stats.active_seconds', -1)]),

                # Achievement and progression
                IndexModel([('guild_id', 1), ('achievements.unlocked_count', -1)]),
                IndexModel([('guild_id', 1), ('prestige_level', -1)]),

                # Time-based analytics
                IndexModel([('created_at', 1)]),
                IndexModel([('last_voice_activity', -1)]),

                # Daily tracking for cleanup operations
                IndexModel([('message_stats.today_key', 1)]),
                IndexModel([('voice_stats.today_key', 1)]),

                # Social and quality metrics
                IndexModel([('guild_id', 1), ('social_stats.helpfulness_rating', -1)]),
                IndexModel([('guild_id', 1), ('quality_stats.average_score', -1)])
            ]
        )

        # Whitelist collections
        self._collection_configs['serverdata_whitelist'] = CollectionConfig(
            name='Whitelist',
            database='Server-Data',
            connection='secondary',
            indexes=[
                # Unique whitelist entry per guild and user
                IndexModel([('guild_id', 1), ('user_id', 1)], unique=True, name='guild_user_unique'),
                # Lookup by guild for listing
                IndexModel([('guild_id', 1), ('added_at', -1)], name='guild_added_at'),
                # Lookup by user ID for quick checks
                IndexModel([('user_id', 1)], name='user_id_lookup'),
                # Lookup by username (case-sensitive) for resolution
                IndexModel([('guild_id', 1), ('username', 1)], name='guild_username_lookup'),
                # Find active whitelisted users
                IndexModel([('guild_id', 1), ('is_active', 1)], name='guild_active'),
                # Track by who added them
                IndexModel([('added_by', 1), ('added_at', -1)], name='added_by_time')
            ]
        )