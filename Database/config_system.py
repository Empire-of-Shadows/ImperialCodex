import json
import os
from typing import Any, Dict, List, Set, Type
from dataclasses import dataclass
from utils.logger import get_logger
import yaml

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

@dataclass
class ConfigDefinition:
    name: str
    type: Type
    default: Any
    description: str = ""
    validator: callable = None


class BotConfig:
    def __init__(self, config_path: str = "Database/config"):
        logger.info(f"Initializing BotConfig with path: {config_path}")
        self.config_path = config_path
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

        logger.debug(f"Defined {len(self._config_definitions)} configuration settings")

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

    # Methods to modify config
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

    def _notify_callbacks(self):
        """Notify all callbacks of config changes"""
        logger.debug(f"Notifying {len(self._callbacks)} callbacks of config changes")
        for callback in self._callbacks:
            try:
                callback(self._values)
                logger.debug(f"Successfully executed callback: {callback.__name__}")
            except Exception as e:
                logger.error(f"Error in config callback {callback.__name__}: {e}", exc_info=True)

# Optional: Add callback to log config changes
def on_config_change(new_config):
    logger.info("Configuration was updated")



config = BotConfig()
config.add_callback(on_config_change)
