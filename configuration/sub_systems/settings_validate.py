from typing import Any, Dict, Optional

from utils.logger import get_logger

logger = get_logger("SettingsValidater")

class SettingsValidater:
    def _validate_role_tier_mapping(self, value: Any) -> bool:
        """Validate role to tier mapping"""
        if not isinstance(value, dict):
            logger.warning("Role tier mapping validation failed: not a dict")
            return False
        for role_id, tiers in value.items():
            if not (isinstance(role_id, str) and role_id.isdigit()):
                logger.warning(f"Role tier mapping validation failed: invalid role_id '{role_id}'")
                return False
            if not (isinstance(tiers, list) and all(isinstance(t, str) for t in tiers)):
                logger.warning(f"Role tier mapping validation failed: invalid tiers for role_id '{role_id}'")
                return False
        logger.debug("Role tier mapping validation passed")
        return True

    def _validate_role_limits(self, value: Any) -> bool:
        """Validate role description limits"""
        if not isinstance(value, dict):
            logger.warning("Role limits validation failed: not a dict")
            return False
        for role_id, limit in value.items():
            if not (isinstance(role_id, str) and role_id.isdigit()):
                logger.warning(f"Role limits validation failed: invalid role_id '{role_id}'")
                return False
            if not (isinstance(limit, int) and 1 <= limit <= 4000):
                logger.warning(f"Role limits validation failed: invalid limit {limit} for role_id '{role_id}'")
                return False
        logger.debug("Role limits validation passed")
        return True

    def _validate_color_tiers(self, value: Any) -> bool:
        """Validate color tiers configuration with hex strings"""
        if not isinstance(value, dict):
            logger.warning("Color tiers validation failed: not a dict")
            return False

        for tier_name, colors in value.items():
            if not isinstance(tier_name, str):
                logger.warning(f"Color tiers validation failed: invalid tier_name type")
                return False
            if not isinstance(colors, dict):
                logger.warning(f"Color tiers validation failed: colors for '{tier_name}' not a dict")
                return False
            for color_name, hex_value in colors.items():
                if not isinstance(color_name, str):
                    logger.warning(f"Color tiers validation failed: invalid color_name in '{tier_name}'")
                    return False
                # Allow both int and string formats
                if isinstance(hex_value, int):
                    if not (0 <= hex_value <= 0xFFFFFF):
                        logger.warning(
                            f"Color tiers validation failed: hex value {hex_value} out of range in '{tier_name}'")
                        return False
                elif isinstance(hex_value, str):
                    if not (hex_value.startswith('#') and len(hex_value) == 7):
                        logger.warning(
                            f"Color tiers validation failed: invalid hex string '{hex_value}' in '{tier_name}'")
                        return False
                    try:
                        int(hex_value[1:], 16)
                    except ValueError:
                        logger.warning(
                            f"Color tiers validation failed: invalid hex value '{hex_value}' in '{tier_name}'")
                        return False
                else:
                    logger.warning(f"Color tiers validation failed: invalid hex_value type in '{tier_name}'")
                    return False
        logger.debug("Color tiers validation passed")
        return True

    def _validate_feature_access(self, value: Any) -> bool:
        """Validate feature access configuration"""
        if not isinstance(value, dict):
            logger.warning("Feature access validation failed: not a dict")
            return False
        for feature_name, role_ids in value.items():
            if not isinstance(feature_name, str):
                logger.warning(f"Feature access validation failed: invalid feature_name type")
                return False
            if not (isinstance(role_ids, list) and all(isinstance(r, str) and r.isdigit() for r in role_ids)):
                logger.warning(f"Feature access validation failed: invalid role_ids for feature '{feature_name}'")
                return False
        logger.debug("Feature access validation passed")
        return True
    def _validate_archive_duration(self, value: Any) -> int:
        """Validate thread auto-archive duration"""
        if not isinstance(value, int):
            raise ValueError(f"Archive duration must be an integer, got {type(value)}")

        valid_durations = {60, 1440, 4320, 10080}
        if value not in valid_durations:
            raise ValueError(f"Archive duration must be one of {valid_durations}, got {value}")

        return value

    def _validate_optional_channel_id(self, value: Any) -> Optional[int]:
        """Validate optional channel ID (can be None or integer)"""
        if value is None:
            return None

        if not isinstance(value, int):
            raise ValueError(f"Channel ID must be an integer or None, got {type(value)}")

        if value <= 0:
            raise ValueError(f"Channel ID must be positive, got {value}")

        return value

    def _validate_bool(self, value: Any) -> bool:
        """Validate boolean value"""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            if value.lower() in ('true', 'yes', '1', 'on'):
                return True
            elif value.lower() in ('false', 'no', '0', 'off'):
                return False

        raise ValueError(f"Value must be a boolean, got {type(value)}: {value}")

    def _validate_string(self, value: Any) -> str:
        """Validate string value"""
        if not isinstance(value, str):
            raise ValueError(f"Value must be a string, got {type(value)}")
        return value

    def _validate_announcement_thread_config(self, value: Any) -> Dict[str, Any]:
        """Validate nested announcement thread configuration"""
        if not isinstance(value, dict):
            raise ValueError(f"Announcement thread config must be a dictionary, got {type(value)}")

        # Set defaults for missing keys
        validated = value.copy()

        # Validate enabled
        if "enabled" not in validated:
            validated["enabled"] = True
        else:
            validated["enabled"] = self._validate_bool(validated["enabled"])

        # Validate channel_id (optional)
        if "channel_id" in validated and validated["channel_id"] is not None:
            validated["channel_id"] = self._validate_optional_channel_id(validated["channel_id"])

        # Validate name_format
        if "name_format" not in validated:
            validated["name_format"] = "💬 {message_content}"
        else:
            validated["name_format"] = self._validate_string(validated["name_format"])

        # Validate auto_archive_duration
        if "auto_archive_duration" not in validated:
            validated["auto_archive_duration"] = 1440
        else:
            validated["auto_archive_duration"] = self._validate_archive_duration(validated["auto_archive_duration"])

        # Validate welcome_message
        if "welcome_message" not in validated:
            validated["welcome_message"] = "💬 **Discussion Thread**\n\nDiscuss this announcement here!"
        else:
            validated["welcome_message"] = self._validate_string(validated["welcome_message"])

        # Validate auto_delete_threads
        if "auto_delete_threads" not in validated:
            validated["auto_delete_threads"] = True
        else:
            validated["auto_delete_threads"] = self._validate_bool(validated["auto_delete_threads"])

        return validated

    def _validate_and_load(self, config_dict: Dict[str, Any]):
        """Validate and load configuration"""
        logger.debug("Validating configuration data")
        errors = []

        for key, definition in self._config_definitions.items():
            if key not in config_dict:
                if definition.default is not None:
                    self._values[key] = definition.default
                    logger.debug(f"Using default value for missing config key: {key}")
                else:
                    error_msg = f"Missing required config: {key}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                continue

            value = config_dict[key]
            if definition.validator and not definition.validator(value):
                error_msg = f"Invalid value for {key}: {value}"
                errors.append(error_msg)
                logger.error(error_msg)
            else:
                self._values[key] = value
                logger.debug(f"Loaded config value for: {key}")

        if errors:
            error_summary = f"Config validation errors:\n" + "\n".join(errors)
            logger.error(error_summary)
            raise ValueError(error_summary)

        logger.info(f"Successfully validated and loaded {len(self._values)} configuration values")