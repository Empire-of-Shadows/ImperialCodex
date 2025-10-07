import json
import os
from typing import Set, Dict

from utils.logger import get_logger

logger = get_logger("SettingsUpdate")

class SettingsUpdate:
    def save_config(self):
        """Save current configuration to file"""
        logger.info(f"Saving configuration to: {self.config_path}")
        try:
            # Only save to file paths, not directories
            if os.path.isdir(self.config_path):
                logger.warning(f"Cannot save to directory path: {self.config_path}. Skipping save operation.")
                return

            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self._values, f, indent=2, ensure_ascii=False)
            logger.debug(f"Configuration saved successfully with {len(self._values)} values")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}", exc_info=True)
            raise

    def update_role_tier(self, role_id: int, tiers: Set[str]):
        """Update tier mapping for a role"""
        logger.info(f"Updating role tier for role_id {role_id} with tiers: {tiers}")
        try:
            current = self._values.get("role_to_tier_mapping", {})
            current[str(role_id)] = list(tiers)
            self._values["role_to_tier_mapping"] = current
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated role tier for role_id {role_id}")
        except Exception as e:
            logger.error(f"Failed to update role tier for role_id {role_id}: {e}", exc_info=True)
            raise

    def update_role_limit(self, role_id: int, limit: int):
        """Update description limit for a role"""
        capped_limit = min(limit, 4000)
        logger.info(f"Updating role limit for role_id {role_id}: {limit} (capped: {capped_limit})")
        try:
            current = self._values.get("role_description_limits", {})
            current[str(role_id)] = capped_limit
            self._values["role_description_limits"] = current
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated role limit for role_id {role_id}")
        except Exception as e:
            logger.error(f"Failed to update role limit for role_id {role_id}: {e}", exc_info=True)
            raise

    def update_max_cache_entries(self, max_entries: int):
        """Update maximum cache entries"""
        logger.info(f"Updating max_cache_entries to: {max_entries}")
        try:
            # Validation happens automatically through the ConfigDefinition validator
            self._values["max_cache_entries"] = max_entries
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated max_cache_entries to: {max_entries}")
        except Exception as e:
            logger.error(f"Failed to update max_cache_entries: {e}", exc_info=True)
            raise

    def update_cache_duration(self, duration: int):
        """Update cache duration in seconds"""
        logger.info(f"Updating cache_duration to: {duration} seconds")
        try:
            # Validation happens automatically through the ConfigDefinition validator
            self._values["cache_duration"] = duration
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated cache_duration to: {duration} seconds")
        except Exception as e:
            logger.error(f"Failed to update cache_duration: {e}", exc_info=True)
            raise

    def update_tier_colors(self, tier: str, colors: Dict[str, int]):
        """Update colors for a specific tier"""
        logger.info(f"Updating colors for tier '{tier}' with {len(colors)} colors")
        try:
            current = self._values.get("color_tiers", {})
            current[tier] = colors
            self._values["color_tiers"] = current
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated colors for tier '{tier}'")
        except Exception as e:
            logger.error(f"Failed to update colors for tier '{tier}': {e}", exc_info=True)
            raise

    def update_feature_roles(self, feature: str, role_ids: Set[int]):
        """Update which roles have access to a feature"""
        logger.info(f"Updating feature access for '{feature}' with {len(role_ids)} roles")
        try:
            current = self._values.get("feature_access", {})
            current[feature] = [str(rid) for rid in role_ids]
            self._values["feature_access"] = current
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated feature access for '{feature}'")
        except Exception as e:
            logger.error(f"Failed to update feature access for '{feature}': {e}", exc_info=True)
            raise

    async def update_channel_id(self, channel_type: str, channel_id: int, channel_name: str = None, bot=None):
        """
        Update a channel ID and optionally store its name.

        Args:
            channel_type: Either "suggestion" or "admin"
            channel_id: The Discord channel ID
            channel_name: Optional channel name for reference
            bot: Optional bot instance to fetch channel info
        """
        logger.info(f"Updating {channel_type}_channel_id to: {channel_id}")

        try:
            config_key = f"{channel_type}_channel_id"

            if config_key not in self._config_definitions:
                raise ValueError(f"Unknown channel type: {channel_type}")

            # Validate channel_id
            if not isinstance(channel_id, int) or channel_id <= 0:
                raise ValueError(f"Invalid channel ID: {channel_id}")

            # If bot is provided and channel_name is not given, try to fetch channel name
            if bot and not channel_name:
                channel = bot.get_channel(channel_id)
                if channel:
                    channel_name = channel.name
                    logger.debug(f"Fetched channel name: {channel_name}")
                else:
                    logger.warning(f"Could not fetch channel name for ID: {channel_id}")

            # Update the channel ID
            self._values[config_key] = channel_id

            # Update channel names mapping if name is provided
            if channel_name:
                current_names = self._values.get("channel_names", {})
                current_names[str(channel_id)] = channel_name
                self._values["channel_names"] = current_names
                logger.debug(f"Updated channel name mapping: {channel_id} -> {channel_name}")

            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated {channel_type}_channel_id to: {channel_id}")

        except Exception as e:
            logger.error(f"Failed to update {channel_type}_channel_id: {e}", exc_info=True)
            raise
    def _notify_callbacks(self):
        """Notify all callbacks of config changes"""
        logger.debug(f"Notifying {len(self._callbacks)} callbacks of config changes")
        for callback in self._callbacks:
            try:
                callback(self._values)
                logger.debug(f"Successfully executed callback: {callback.__name__}")
            except Exception as e:
                logger.error(f"Error in config callback {callback.__name__}: {e}", exc_info=True)