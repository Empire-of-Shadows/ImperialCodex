import json
import os
import time
from typing import Any, Dict, List, Set, Type, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

from Database.sub_systems.settings_define import SettingsDefine, ConfigDefinition
from Database.sub_systems.settings_update import SettingsUpdate
from Database.sub_systems.settings_validate import SettingsValidater
from utils.logger import get_logger
import yaml

load_dotenv()

config_dir = os.getenv("CONFIG_DIR")
# Initialize logger for this module
logger = get_logger("config_system")

S = " " * 50

def format_value_for_logging(value: Any, max_single_line: int = 80) -> str:
    """Format values nicely for logging output with smart formatting."""
    if isinstance(value, (dict, list)):
        # Convert to pretty JSON
        pretty_json = json.dumps(value, indent=2, ensure_ascii=False)

        # Check if it's small enough for single line
        compact_str = json.dumps(value, ensure_ascii=False)
        if len(compact_str) <= max_single_line:
            return f" {compact_str}"

        # For multi-line, add proper indentation and {s} at the start of each line
        indented = pretty_json.replace('\n', f'\n{S}')
        return f" \n{S}{S}{indented}"

    return f" {value}"

class BotConfig(SettingsValidater, SettingsDefine, SettingsUpdate):
    def __init__(self, config_path: str = config_dir):
        logger.info(f"Initializing BotConfig with path: {config_path}")
        self.config_path = config_path
        # Debugging: List files in config directory
        if os.path.exists(config_path):
            logger.debug(f"Files in {config_path}: {os.listdir(config_path)}")
        else:
            logger.warning(f"Directory {config_path} does not exist.")

        self._config_definitions: Dict[str, ConfigDefinition] = {}
        self._values: Dict[str, Any] = {}
        self._callbacks: List[callable] = []

        try:
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            logger.debug(f"Ensured config directory exists: {os.path.dirname(config_path)}")
        except Exception as e:
            logger.error(f"Failed to create config directory: {e}", exc_info=True)
            raise

        self._define_settings()
        self.load_config()
        logger.info("BotConfig initialization completed successfully")

    def _load_file(self, file_path: str) -> Dict[str, Any]:
        """Load a single configuration file (JSON or YAML/YML)"""
        logger.debug(f"Loading configuration file: {file_path}")

        try:
            if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                    logger.debug(
                        f"YAML - Successfully loaded '{file_path}' with {len(config_data) if config_data else 0} keys")
                    return config_data if config_data else {}

            elif file_path.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    logger.debug(f"JSON - Successfully loaded '{file_path}' with {len(config_data)} keys")
                    return config_data

            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return {}

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON file '{file_path}': {e}", exc_info=True)
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML file '{file_path}': {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error loading file '{file_path}': {e}", exc_info=True)
            raise

    def _merge_configs(self, base_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two configuration dictionaries deeply"""
        merged = base_config.copy()

        for key, value in new_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                # Deep merge for nested dictionaries
                merged[key] = self._merge_configs(merged[key], value)
            else:
                # Override or add new key
                merged[key] = value

        return merged

    def load_config(self):
        """Load configuration from file(s) - supports single file or directory with multiple files"""
        logger.info(f"Loading configuration from: {self.config_path}")

        if not os.path.exists(self.config_path):
            logger.warning(f"Configuration path not found, creating default: {self.config_path}")
            self._create_default_config()
            return

        try:
            file_config = {}

            # Load from a directory - walk through and merge all config files
            if os.path.isdir(self.config_path):
                logger.debug(f"Loading configuration from directory: {self.config_path}")
                config_files = []

                # Collect all valid config files
                for root, _, files in os.walk(self.config_path):
                    for file in files:
                        if file.endswith(('.yaml', '.yml', '.json')):
                            config_files.append(os.path.join(root, file))

                if not config_files:
                    logger.warning(f"No configuration files found in directory: {self.config_path}")
                    self._create_default_config()
                    return

                logger.info(f"Found {len(config_files)} configuration file(s) in directory")

                # Load and merge all config files
                for config_file in sorted(config_files):  # Sort for consistent load order
                    try:
                        loaded_config = self._load_file(config_file)
                        file_config = self._merge_configs(file_config, loaded_config)
                        logger.debug(f"Merged configuration from: {os.path.basename(config_file)}")
                        logger.debug(f"Successfully merged {len(loaded_config)} configuration key(s)")
                        for key, value in file_config.items():
                            formatted_value = format_value_for_logging(value)
                            logger.debug(f"Loaded key '{key}' with value: {formatted_value}")
                    except Exception as e:
                        logger.error(f"Failed to load config file '{config_file}': {e}", exc_info=True)
                        raise

                logger.info(f"Successfully merged {len(config_files)} configuration file(s)")

            # Load from a single YAML file
            elif self.config_path.endswith(('.yaml', '.yml')):
                logger.debug(f"Loading configuration from single YAML file: {self.config_path}")
                file_config = self._load_file(self.config_path)
                for key, value in file_config.items():
                    formatted_value = format_value_for_logging(value)
                    logger.debug(f"\n"
                                 f"YAML - Loaded key\n"
                                 f" '{key}':"
                                 f"{formatted_value}")
                logger.debug(f"YAML - Successfully loaded '{self.config_path}' with {len(file_config)} keys")

            # Load from a single JSON file
            elif self.config_path.endswith('.json'):
                logger.debug(f"Loading configuration from single JSON file: {self.config_path}")
                file_config = self._load_file(self.config_path)
                # Log the key values
                for key, value in file_config.items():
                    formatted_value = format_value_for_logging(value)
                    logger.debug(f"JSON - Loaded key '{key}' with value: {formatted_value}")
                logger.debug(f"JSON - Successfully loaded '{self.config_path}' with {len(file_config)} keys")

            # Unsupported file extension
            else:
                logger.warning(
                    f"Unknown file extension for configuration file: {self.config_path}. "
                    f"Supported extensions: .yaml, .yml, .json"
                )
                self._create_default_config()
                return

            # Validate and load the merged configuration
            self._validate_and_load(file_config)
            logger.info(f"Configuration loaded and validated successfully with {len(file_config)} top-level keys")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration: {e}", exc_info=True)
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}", exc_info=True)
            raise

    def _create_default_config(self):
        """Create default config file"""
        logger.info("Creating default configuration")
        try:
            self._values = {key: definition.default for key, definition in self._config_definitions.items()}
            self.save_config()
            logger.info(f"Default configuration created successfully at: {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to create default configuration: {e}", exc_info=True)
            raise

    # Property accessors for easy usage
    @property
    def role_to_tier_mapping(self) -> Dict[int, Set[str]]:
        """Get role to tier mapping with proper types"""
        raw = self._values.get("role_to_tier_mapping", {})
        result = {int(role_id): set(tiers) for role_id, tiers in raw.items()}
        logger.debug(f"Retrieved role_to_tier_mapping with {len(result)} entries")
        return result

    @property
    def role_description_limits(self) -> Dict[int, int]:
        """Get role description limits with proper types"""
        raw = self._values.get("role_description_limits", {})
        result = {int(role_id): limit for role_id, limit in raw.items()}
        logger.debug(f"Retrieved role_description_limits with {len(result)} entries")
        return result

    @property
    def max_cache_entries(self) -> int:
        """Get maximum cache entries"""
        return self._values.get("max_cache_entries", 1000)

    @property
    def cache_duration(self) -> int:
        """Get cache duration in seconds"""
        return self._values.get("cache_duration", 3600)

    @property
    def color_tiers(self) -> Dict[str, Dict[str, int]]:
        """Get color tiers with hex strings converted to integers"""
        raw_tiers = self._values.get("color_tiers", {})
        processed_tiers = {}

        for tier_name, colors in raw_tiers.items():
            processed_tiers[tier_name] = {}
            for color_name, hex_value in colors.items():
                if isinstance(hex_value, str) and hex_value.startswith('#'):
                    processed_tiers[tier_name][color_name] = int(hex_value[1:], 16)
                else:
                    processed_tiers[tier_name][color_name] = hex_value

        logger.debug(f"Retrieved color_tiers with {len(processed_tiers)} tiers")
        return processed_tiers

    @property
    def default_description_limit(self) -> int:
        return self._values.get("default_description_limit", 500)

    @property
    def feature_access(self) -> Dict[str, Set[int]]:
        """Get feature access with proper types"""
        raw = self._values.get("feature_access", {})
        result = {feature: {int(role_id) for role_id in role_ids} for feature, role_ids in raw.items()}
        logger.debug(f"Retrieved feature_access with {len(result)} features")
        return result

    """
    ```yaml
    # Channel the suggestions will be sent to
    suggestion_channel_id: 1371239792888516719

    # Channel to get a copy of the suggestions
    admin_channel_id: 1265125349772230787
    ```
    """

    @property
    def suggestion_channel_id(self) -> Optional[int]:
        """Get suggestion channel ID"""
        return self._values.get("suggestion_channel_id")

    @property
    def admin_channel_id(self) -> Optional[int]:
        """Get admin channel ID"""
        return self._values.get("admin_channel_id")

    @property
    def channel_names(self) -> Dict[int, str]:
        """Get channel ID to name mapping"""
        raw = self._values.get("channel_names", {})
        return {int(channel_id): name for channel_id, name in raw.items()}

    def get_channel_name(self, channel_id: int) -> Optional[str]:
        """Get stored name for a channel ID"""
        return self.channel_names.get(channel_id)

    def get_suggestion_channel_id(self) -> Optional[int]:
        """Get suggestion channel ID (convenience method)"""
        return self.suggestion_channel_id

    def get_admin_channel_id(self) -> Optional[int]:
        """Get admin channel ID (convenience method)"""
        return self.admin_channel_id

    def get_description_limit_for_role(self, role_id: int) -> int:
        """Get the description limit for a specific role"""
        limit = self.role_description_limits.get(role_id, self.default_description_limit)
        logger.debug(f"Description limit for role_id {role_id}: {limit}")
        return limit

    def get_tiers_for_role(self, role_id: int) -> Set[str]:
        """Get tiers for a specific role"""
        tiers = self.role_to_tier_mapping.get(role_id, set())
        logger.debug(f"Tiers for role_id {role_id}: {tiers}")
        return tiers

    def get_available_colors(self, user_tiers: Set[str]) -> Dict[str, int]:
        """Get all colors available to a user based on their tiers"""
        available_colors = {}
        for tier in user_tiers:
            if tier in self.color_tiers:
                available_colors.update(self.color_tiers[tier])
        logger.debug(f"Available colors for tiers {user_tiers}: {len(available_colors)} colors")
        return available_colors

    def get_tier_colors(self, tier: str) -> Dict[str, int]:
        """Get all colors for a specific tier"""
        colors = self.color_tiers.get(tier, {})
        logger.debug(f"Colors for tier '{tier}': {len(colors)} colors")
        return colors

    def can_access_feature(self, role_id: int, feature: str) -> bool:
        """Check if a role can access a feature"""
        can_access = role_id in self.feature_access.get(feature, set())
        logger.debug(f"Role {role_id} can access feature '{feature}': {can_access}")
        return can_access

    def add_callback(self, callback: callable):
        """Add callback for config changes"""
        self._callbacks.append(callback)
        logger.debug(f"Added callback: {callback.__name__}. Total callbacks: {len(self._callbacks)}")


# Optional: Add callback to log config changes
def on_config_change(new_config):
    logger.info("Configuration was updated")



config = BotConfig()
config.add_callback(on_config_change)
