# Python
import logging
import re
from typing import Optional, Set, Callable, Awaitable
import discord

from configuration.config_system import config
from utils.logger import get_logger, log_context, log_performance

logger = get_logger("EmbedModal", level=logging.INFO, colored_console=True)

@log_performance("get_max_description_length")
def get_max_description_length(user_roles: Set[int]) -> int:
    """
    Return the maximum embed description length allowed for the given roles.
    Roles do not stack; we choose the highest limit among the user's roles.
    The absolute ceiling is aligned to Discord's 4000 TextInput limit.
    """
    with log_context(logger, f"Calculating max description length for {len(user_roles)} roles", level=logging.DEBUG):
        # Use get_description_limit_for_role for each role
        role_limits = [config.get_description_limit_for_role(r) for r in user_roles]
        allowed = max(role_limits, default=config.default_description_limit)
        final_limit = min(allowed, 4000)

        logger.debug(f"Role limits found: {role_limits}, max allowed: {allowed}, final limit: {final_limit}")
        return final_limit


@log_performance("get_allowed_colors")
def get_allowed_colors(user_roles: Set[int]) -> dict[str, int]:
    """Get allowed colors for user based on their roles."""
    with log_context(logger, f"Determining allowed colors for {len(user_roles)} roles", level=logging.DEBUG):
        # Get all tiers from user's roles in one go
        user_tiers = {
            tier
            for role_id in user_roles
            for tier in config.get_tiers_for_role(role_id)
        }

        logger.debug(f"User roles grant access to tiers: {user_tiers}")

        # Get available colors for these tiers
        allowed_colors = config.get_available_colors(user_tiers)

        logger.info(f"User has access to {len(allowed_colors)} colors from tiers: {user_tiers}")
        return allowed_colors

def _is_valid_image_url(url: str) -> bool:
    """Validate if URL is a valid image URL."""
    logger.debug(f"Validating image URL: {url[:50]}{'...' if len(url) > 50 else ''}")

    if not url or not url.startswith("https://"):
        logger.debug(f"URL validation failed: Invalid protocol or empty URL")
        return False

    is_valid = bool(re.search(r"\.(png|jpe?g|gif|webp|bmp|svg)(?:\?.*)?$", url, flags=re.IGNORECASE))
    logger.debug(f"URL validation result: {is_valid}")
    return is_valid


def _parse_color_to_int(color_str: str) -> Optional[int]:
    """
    Parse color string to integer.
    Accepts:
    - #RRGGBB, #RGB
    - 0xRRGGBB
    - RRGGBB
    Returns int or None if invalid.
    """
    logger.debug(f"Parsing color string: '{color_str}'")

    s = color_str.strip().lower()
    if not s:
        logger.debug("Empty color string provided")
        return None

    original_format = "unknown"
    if s.startswith("#"):
        s = s[1:]
        original_format = "hex with #"
    elif s.startswith("0x"):
        s = s[2:]
        original_format = "hex with 0x"
    else:
        original_format = "raw hex"

    if not re.fullmatch(r"[0-9a-f]{3}|[0-9a-f]{6}", s):
        logger.debug(f"Color validation failed: Invalid hex format for '{s}' (original format: {original_format})")
        return None

    if len(s) == 3:
        s = "".join(ch * 2 for ch in s)
        logger.debug(f"Expanded 3-digit hex to 6-digit: {s}")

    try:
        color_int = int(s, 16)
        logger.debug(
            f"Successfully parsed color '{color_str}' ({original_format}) to integer: {color_int} (0x{color_int:06X})")
        return color_int
    except ValueError as e:
        logger.warning(f"Failed to parse color '{color_str}' to integer: {e}")
        return None


class EmbedModal(discord.ui.Modal):
    def __init__(self, user_roles: Set[int],
                 cache_update_callback: Optional[Callable[[int, int], Awaitable[None]]] = None):
        """
        Modal for creating an embed.

        :param user_roles: The roles of the user invoking the modal.
        :param cache_update_callback: A callback function to update the authorization cache.
        """
        with log_context(logger, "Initializing EmbedModal", level=logging.DEBUG):
            super().__init__(title="Create Embed")

            # Log initialization details
            logger.info(f"EmbedModal initialized with {len(user_roles)} user roles: {list(user_roles)}")
            logger.debug(f"Cache update callback provided: {cache_update_callback is not None}")

            # Centralized role-based maximum description length (non-stacking, capped to 4000)
            max_length = get_max_description_length(user_roles)
            logger.debug(f"Modal configured with max description length: {max_length} characters")

            # Title input
            self.title_input = discord.ui.TextInput(
                label="Title",
                placeholder="Enter embed title (optional)",
                required=False,
                max_length=256,
            )
            self.add_item(self.title_input)
            logger.debug("Title input field added to modal")

            # Description input
            self.description_input = discord.ui.TextInput(
                label="Description",
                placeholder=f"Enter embed description (up to {max_length} characters)",
                style=discord.TextStyle.paragraph,
                required=True,
                max_length=max_length,
            )
            self.add_item(self.description_input)
            logger.debug(f"Description input field added with max_length: {max_length}")

            # Thumbnail input
            self.thumbnail_input = discord.ui.TextInput(
                label="Thumbnail URL",
                placeholder="Enter a valid image URL or type 'none' to skip/remove (optional)",
                required=False,
            )
            self.add_item(self.thumbnail_input)
            logger.debug("Thumbnail input field added to modal")

            # Color input
            self.color_input = discord.ui.TextInput(
                label="Color",
                placeholder="Enter hex (e.g., #FF0000) or allowed name",
                required=False,
                max_length=32,
            )
            self.add_item(self.color_input)
            logger.debug("Color input field added to modal")

            self.user_roles = user_roles
            self.cache_update_callback = cache_update_callback

            logger.info("EmbedModal initialization completed successfully")

    @log_performance("embed_modal_submission")
    async def on_submit(self, interaction: discord.Interaction):
        """
        Handle embed creation on modal submission.
        """
        operation_id = f"embed_{interaction.user.id}_{hash(str(interaction.created_at)) % 10000}"

        with log_context(logger, f"Processing embed modal submission [{operation_id}]"):
            logger.info(
                f"Modal submitted by {interaction.user} ({interaction.user.id}) in guild {interaction.guild} ({interaction.guild.id if interaction.guild else 'DM'})")

            try:
                # Get allowed colors for validation
                with log_context(logger, "Retrieving user permissions and colors", level=logging.DEBUG):
                    allowed_colors = get_allowed_colors(self.user_roles)
                    logger.debug(f"User has access to {len(allowed_colors)} allowed colors")

                # Retrieve and log field values
                with log_context(logger, "Processing input field values", level=logging.DEBUG):
                    title = self.title_input.value or None
                    description = self.description_input.value
                    thumbnail_url_raw = (self.thumbnail_input.value or "").strip()
                    color_input_raw = (self.color_input.value or "").strip()

                    logger.debug(f"Input values - Title: {'Set' if title else 'None'}, "
                                 f"Description: {len(description)} chars, "
                                 f"Thumbnail: {'Set' if thumbnail_url_raw else 'None'}, "
                                 f"Color: '{color_input_raw}' {'Set' if color_input_raw else 'None'}")

                # Validate and process color
                with log_context(logger, "Processing color validation"):
                    embed_color_val: int
                    if not color_input_raw:
                        embed_color_val = 0x000000  # Default
                        logger.debug("Using default color (black) as no color was specified")
                    else:
                        lower_key = color_input_raw.lower()
                        if lower_key in allowed_colors:
                            embed_color_val = allowed_colors[lower_key]
                            logger.info(f"Using named color '{color_input_raw}' -> 0x{embed_color_val:06X}")
                        else:
                            parsed = _parse_color_to_int(color_input_raw)
                            if parsed is None:
                                error_msg = f"Invalid color '{color_input_raw}'. Please use a valid hex code or allowed name."
                                logger.warning(f"Color validation failed for user {interaction.user}: {error_msg}")
                                await interaction.response.send_message(f"❌ {error_msg}", ephemeral=True)
                                return
                            if allowed_colors and parsed not in allowed_colors.values():
                                error_msg = f"You are not authorized to use the color '{color_input_raw}'."
                                logger.warning(
                                    f"Color authorization failed for user {interaction.user}: {error_msg} - Color: 0x{parsed:06X}")
                                await interaction.response.send_message(f"❌ {error_msg}", ephemeral=True)
                                return
                            embed_color_val = parsed
                            logger.info(f"Using parsed color '{color_input_raw}' -> 0x{embed_color_val:06X}")

                # Create embed
                with log_context(logger, "Creating Discord embed"):
                    embed = discord.Embed(title=title, description=description, color=embed_color_val)
                    logger.debug(
                        f"Base embed created - Title: {bool(title)}, Description: {len(description)} chars, Color: 0x{embed_color_val:06X}")

                # Set thumbnail logic
                with log_context(logger, "Processing thumbnail configuration"):
                    if thumbnail_url_raw.lower() == "none":
                        # Explicitly do not set thumbnail
                        logger.debug("Thumbnail explicitly set to 'none' - no thumbnail will be added")
                    elif thumbnail_url_raw:
                        if not _is_valid_image_url(thumbnail_url_raw):
                            error_msg = "Invalid thumbnail URL. Provide a valid http(s) image URL or 'none'."
                            logger.warning(
                                f"Thumbnail validation failed for user {interaction.user}: {error_msg} - URL: {thumbnail_url_raw[:100]}")
                            await interaction.response.send_message(f"❌ {error_msg}", ephemeral=True)
                            return
                        embed.set_thumbnail(url=thumbnail_url_raw)
                        logger.info(
                            f"Thumbnail set from provided URL: {thumbnail_url_raw[:50]}{'...' if len(thumbnail_url_raw) > 50 else ''}")
                    else:
                        # Fallback to user's avatar if available
                        avatar = interaction.user.avatar
                        if avatar:
                            embed.set_thumbnail(url=avatar.url)
                            logger.debug(f"Thumbnail set to user's avatar: {avatar.url}")
                        else:
                            embed.set_thumbnail(url=interaction.user.default_avatar.url)
                            logger.debug(
                                f"Thumbnail set to user's default avatar: {interaction.user.default_avatar.url}")

                # Send embed
                with log_context(logger, "Sending embed message"):
                    await interaction.response.defer()
                    message = await interaction.followup.send(embed=embed)
                    logger.info(
                        f"Embed message sent successfully - Message ID: {message.id}, Channel: {message.channel.name if hasattr(message.channel, 'name') else 'DM'}")

                # Update cache if callback provided
                if self.cache_update_callback:
                    with log_context(logger, "Updating authorization cache", level=logging.DEBUG):
                        await self.cache_update_callback(interaction.user.id, message.id)
                        logger.debug(
                            f"Cache update callback executed for user {interaction.user.id}, message {message.id}")

                # Comprehensive audit logging
                audit_data = {
                    'operation_id': operation_id,
                    'action': 'embed_create',
                    'user': {
                        'id': interaction.user.id,
                        'name': str(interaction.user),
                        'discriminator': interaction.user.discriminator,
                        'roles': list(self.user_roles)
                    },
                    'guild': {
                        'id': interaction.guild.id if interaction.guild else None,
                        'name': interaction.guild.name if interaction.guild else None
                    },
                    'embed_data': {
                        'title': title,
                        'description_length': len(description),
                        'color': f"0x{embed_color_val:06X}",
                        'thumbnail': 'custom' if thumbnail_url_raw and thumbnail_url_raw.lower() != 'none' else 'avatar' if not thumbnail_url_raw else 'none',
                        'message_id': message.id
                    },
                    'permissions': {
                        'max_description_length': get_max_description_length(self.user_roles),
                        'allowed_color_count': len(allowed_colors)
                    },
                    'timestamp': discord.utils.utcnow().isoformat()
                }

                logger.info(f"Embed creation audit: {audit_data}")

            except discord.Forbidden as e:
                error_msg = f"Permission error during embed submission by {interaction.user}"
                logger.error(f"{error_msg}: {e}", exc_info=True)
                await self._send_error_response(interaction,
                                                "❌ I don't have permission to send embeds in this channel.")
            except discord.HTTPException as e:
                error_msg = f"Discord API error during embed submission by {interaction.user}"
                logger.error(f"{error_msg}: HTTP {e.status} - {e.text}", exc_info=True)
                await self._send_error_response(interaction, "❌ Failed to send embed due to Discord API error.")

            except Exception as e:
                error_msg = f"Unexpected error during embed submission by {interaction.user}"
                logger.exception(f"{error_msg}: {e}")
                await self._send_error_response(interaction,
                                                "❌ An unexpected error occurred while processing your request.")

    async def _send_error_response(self, interaction: discord.Interaction, message: str):
        """Helper method to send error responses, handling both cases where response is done or not."""
        try:
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
                logger.debug("Error message sent via followup")
            else:
                await interaction.response.send_message(message, ephemeral=True)
                logger.debug("Error message sent via response")
        except Exception as e:
            logger.error(f"Failed to send error response to user {interaction.user}: {e}")