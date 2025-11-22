from dataclasses import dataclass
from typing import List, Dict, Type, Any, Optional

from utils.logger import get_logger

logger = get_logger("SettingsDefine")

@dataclass
class ConfigDefinition:
    name: str
    type: Type
    default: Any
    description: str = ""
    validator: callable = None

class SettingsDefine:

    def _define_settings(self):
        """Define all config settings with clean defaults"""
        logger.debug("Defining configuration settings")

        # Role to Tier Mapping
        self._config_definitions["role_to_tier_mapping"] = ConfigDefinition(
            name="role_to_tier_mapping",
            type=Dict[str, List[str]],
            default={},  # Empty by default - will be populated from config file
            description="Maps role IDs to tier sets for feature access. Format: {'role_id': ['tier_1', 'tier_2']}",
            validator=self._validate_role_tier_mapping
        )

        # Role Description Limits
        self._config_definitions["role_description_limits"] = ConfigDefinition(
            name="role_description_limits",
            type=Dict[str, int],
            default={},  # Empty by default
            description="Character limits for descriptions per role. Format: {'role_id': 1000}",
            validator=self._validate_role_limits
        )

        # Color Tiers
        self._config_definitions["color_tiers"] = ConfigDefinition(
            name="color_tiers",
            type=Dict[str, Dict[str, int]],
            default={
                "tier_1": {},
                "tier_2": {},
                "tier_3": {},
                "tier_4": {}
            },
            description="Available colors for each tier. Format: {'tier_name': {'color_name': hex_code}}",
            validator=self._validate_color_tiers
        )

        # Default Description Limit
        self._config_definitions["default_description_limit"] = ConfigDefinition(
            name="default_description_limit",
            type=int,
            default=500,
            description="Default character limit when no role-specific limit is set",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 4000
        )

        # Feature Access
        self._config_definitions["feature_access"] = ConfigDefinition(
            name="feature_access",
            type=Dict[str, List[str]],
            default={
                "basic_embed": [],
                "image_field": [],
                "advanced_embed": []
            },
            description="Which roles can access which features. Format: {'feature_name': ['role_id1', 'role_id2']}",
            validator=self._validate_feature_access
        )

        # Cache Settings for Embeds
        self._config_definitions["max_cache_entries"] = ConfigDefinition(
            name="max_cache_entries",
            type=int,
            default=1000,
            description="Maximum number of entries in the cache",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 1000000
        )

        self._config_definitions["cache_duration"] = ConfigDefinition(
            name="cache_duration",
            type=int,
            default=300,
            description="Duration in seconds for cache entries to expire",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 3600
        )
        logger.debug(f"Defined {len(self._config_definitions)} configuration settings")

        # Channel Settings
        self._config_definitions["suggestion_channel_id"] = ConfigDefinition(
            name="suggestion_channel_id",
            type=int,
            default=None,
            description="Channel ID where suggestions will be sent",
            validator=lambda x: x is None or (isinstance(x, int) and x > 0)
        )

        self._config_definitions["admin_channel_id"] = ConfigDefinition(
            name="admin_channel_id",
            type=int,
            default=None,
            description="Channel ID where admin copies of suggestions are sent",
            validator=lambda x: x is None or (isinstance(x, int) and x > 0)
        )

        # Optional: Store channel names for reference
        self._config_definitions["channel_names"] = ConfigDefinition(
            name="channel_names",
            type=Dict[str, str],
            default={},
            description="Mapping of channel IDs to names for reference",
            validator=lambda x: isinstance(x, dict) and all(
                isinstance(k, str) and k.isdigit() and isinstance(v, str)
                for k, v in x.items()
            )
        )

        # Announcement Thread Settings (nested structure)
        self._config_definitions["announcement_thread"] = ConfigDefinition(
            name="announcement_thread",
            type=Dict[str, Any],
            default={
                "enabled": True,
                "channel_id": None,
                "name_format": "💬 {message_content}",
                "auto_archive_duration": 1440,
                "welcome_message": "💬 **Discussion Thread**\n\nDiscuss this announcement here!",
                "auto_delete_threads": True  # Add this line
            },
            description="Announcement thread configuration with nested settings",
            validator=self._validate_announcement_thread_config
        )

        # Tag Tracker Settings
        self._config_definitions["tag_tracker"] = ConfigDefinition(
            name="tag_tracker",
            type=Dict[str, Any],
            default={
                "enabled": False,
                "role_id": None,
                "server_tag": None
            },
            description="Tag tracker configuration",
            validator=self._validate_tag_tracker_config
        )