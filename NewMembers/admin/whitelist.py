import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional
from datetime import datetime, timezone

from Database.DatabaseManager import db_manager
from utils.logger import get_logger
from utils.bot import GUILD_ID2

logger = get_logger("WhitelistManager")

# Configuration
WHITELIST_ROLE_NAME = "Whitelisted New Member"
WHITELIST_ROLE_COLOR = discord.Color.blue()
ACCOUNT_AGE_REQUIREMENT_DAYS = 90  # Must match the age check in joining.py


def has_whitelist_permissions_app():
    """App command check for whitelist management permissions."""
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False

        user = interaction.user
        perms = getattr(user, "guild_permissions", None)
        if perms and (perms.manage_roles or perms.administrator):
            return True

        role_names = [role.name.lower() for role in getattr(user, "roles", [])]
        allowed_roles = {'admin', 'moderator', 'staff'}
        return any(name in allowed_roles for name in role_names)

    return app_commands.check(predicate)


class WhitelistReasonModal(discord.ui.Modal, title="Whitelist Member"):
    """Modal for collecting the reason for whitelisting a member."""

    reason = discord.ui.TextInput(
        label="Reason for whitelisting",
        style=discord.TextStyle.paragraph,
        placeholder="Why is this member being whitelisted? (e.g., Friend of active member, known from another community)",
        required=True,
        min_length=10,
        max_length=500
    )

    def __init__(self, cog, user_identifier: str, resolved_member: Optional[discord.Member] = None):
        super().__init__()
        self.cog = cog
        self.user_identifier = user_identifier
        self.resolved_member = resolved_member

    async def on_submit(self, interaction: discord.Interaction):
        """Handle the modal submission."""
        await interaction.response.defer(ephemeral=True)

        # If we already resolved the member, use it; otherwise resolve now
        if self.resolved_member:
            result = await self.cog._add_to_whitelist_internal(
                interaction,
                self.resolved_member,
                str(self.reason)
            )
        else:
            result = await self.cog._add_to_whitelist_internal(
                interaction,
                self.user_identifier,
                str(self.reason)
            )

        if result['success']:
            embed = discord.Embed(
                title="✅ Member Whitelisted",
                description=result['message'],
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            if 'details' in result:
                embed.add_field(name="Details", value=result['details'], inline=False)
        else:
            embed = discord.Embed(
                title="❌ Whitelist Failed",
                description=result['message'],
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc)
            )

        embed.set_footer(text=f"Action by {interaction.user}", icon_url=interaction.user.display_avatar.url)
        await interaction.followup.send(embed=embed, ephemeral=True)


class WhitelistGroup(commands.GroupCog, name="whitelist", description="Manage member whitelist for age restrictions"):
    """
    Group cog providing:
    - /whitelist add <user> - Add a user to whitelist (opens modal for reason)
    - /whitelist remove <user> - Remove a user from whitelist
    - /whitelist list - Show all whitelisted users
    - /whitelist check <user> - Check if a user is whitelisted
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logger.info("WhitelistGroup initialized")

    async def _ensure_whitelist_role(self, guild: discord.Guild) -> discord.Role:
        """Ensure the whitelist role exists, create if needed."""
        # Check if role exists
        existing_role = discord.utils.get(guild.roles, name=WHITELIST_ROLE_NAME)
        if existing_role:
            return existing_role

        # Create the role
        try:
            role = await guild.create_role(
                name=WHITELIST_ROLE_NAME,
                color=WHITELIST_ROLE_COLOR,
                reason="Whitelist role for new members with new accounts",
                mentionable=False,
                hoist=True  # Display separately in member list
            )
            logger.info(f"Created whitelist role in guild {guild.name}")
            return role
        except Exception as e:
            logger.error(f"Failed to create whitelist role: {e}")
            raise

    async def _resolve_user_identifier(self, guild: discord.Guild, identifier: str) -> Optional[tuple[int, str, bool]]:
        """
        Resolve a user identifier (ID or username) to (user_id, username, in_guild).

        Args:
            guild: The Discord guild
            identifier: User ID or username (case-sensitive)

        Returns:
            Tuple of (user_id, username, in_guild) or None if not found
        """
        # Try to parse as user ID
        try:
            user_id = int(identifier)
            # Try to find member in guild
            member = guild.get_member(user_id)
            if member:
                return (user_id, member.name, True)

            # Try to fetch user from Discord API
            try:
                user = await self.bot.fetch_user(user_id)
                return (user_id, user.name, False)
            except discord.NotFound:
                return None
            except Exception as e:
                logger.error(f"Error fetching user {user_id}: {e}")
                return None

        except ValueError:
            # Not a valid ID, treat as username (case-sensitive)
            username = identifier

            # Search in guild members first (case-sensitive)
            for member in guild.members:
                if member.name == username:  # Case-sensitive comparison
                    return (member.id, member.name, True)

            # Username not found in guild
            return None

    async def _add_to_whitelist_internal(self, interaction: discord.Interaction, user_or_id, reason: str) -> dict:
        """
        Internal method to add a user to the whitelist.

        Args:
            interaction: The interaction object
            user_or_id: Either a discord.Member, discord.User, user ID string, or username
            reason: The reason for whitelisting

        Returns:
            Dict with 'success', 'message', and optionally 'details'
        """
        guild = interaction.guild

        try:
            # Resolve the user
            if isinstance(user_or_id, (discord.Member, discord.User)):
                user_id = user_or_id.id
                username = user_or_id.name
                in_guild = isinstance(user_or_id, discord.Member)
                member = user_or_id if in_guild else None
            else:
                # String identifier (ID or username)
                resolution = await self._resolve_user_identifier(guild, str(user_or_id))
                if not resolution:
                    return {
                        'success': False,
                        'message': f"Could not find user: `{user_or_id}`\n\n"
                                   "**Tip:** Usernames are case-sensitive. Make sure you're using the exact username, "
                                   "or use the user ID instead."
                    }

                user_id, username, in_guild = resolution
                member = guild.get_member(user_id) if in_guild else None

            # Check if user is a bot
            if member and member.bot:
                return {
                    'success': False,
                    'message': "Cannot whitelist bot accounts."
                }

            # Get whitelist collection
            whitelist_collection = db_manager.get_collection_manager('serverdata_whitelist')

            # Check if already whitelisted
            existing = await whitelist_collection.find_one({
                'guild_id': guild.id,
                'user_id': user_id
            })

            if existing:
                if existing.get('is_active', True):
                    return {
                        'success': False,
                        'message': f"**{username}** (`{user_id}`) is already whitelisted.\n\n"
                                   f"Added by: <@{existing.get('added_by')}>\n"
                                   f"Date: <t:{int(existing.get('added_at').timestamp())}:F>\n"
                                   f"Reason: {existing.get('reason', 'No reason provided')}"
                    }
                else:
                    # Reactivate
                    await whitelist_collection.update_one(
                        {'guild_id': guild.id, 'user_id': user_id},
                        {'$set': {
                            'is_active': True,
                            'reactivated_at': datetime.now(timezone.utc),
                            'reactivated_by': interaction.user.id,
                            'reactivated_reason': reason
                        }}
                    )
                    return {
                        'success': True,
                        'message': f"**{username}** (`{user_id}`) has been reactivated on the whitelist.",
                        'details': f"**Reason:** {reason}"
                    }

            # Add to whitelist
            whitelist_entry = {
                'guild_id': guild.id,
                'user_id': user_id,
                'username': username,
                'added_by': interaction.user.id,
                'added_by_username': interaction.user.name,
                'added_at': datetime.now(timezone.utc),
                'reason': reason,
                'is_active': True,
                'role_assigned': False,
                'role_assigned_at': None
            }

            await whitelist_collection.insert_one(whitelist_entry)

            # If member is in guild and has a new account, assign the role
            role_assigned = False
            if member:
                account_age = (datetime.now(timezone.utc) - member.created_at).days
                if account_age < ACCOUNT_AGE_REQUIREMENT_DAYS:
                    try:
                        role = await self._ensure_whitelist_role(guild)
                        await member.add_roles(role, reason=f"Whitelisted by {interaction.user}")
                        role_assigned = True

                        # Update database with role info
                        await whitelist_collection.update_one(
                            {'guild_id': guild.id, 'user_id': user_id},
                            {'$set': {
                                'role_assigned': True,
                                'role_assigned_at': datetime.now(timezone.utc),
                                'account_age_at_whitelist': account_age
                            }}
                        )
                        logger.info(f"Assigned whitelist role to {member} (account age: {account_age} days)")
                    except Exception as e:
                        logger.error(f"Failed to assign whitelist role: {e}")

            details = f"**User:** {username} (`{user_id}`)\n**Reason:** {reason}"
            if role_assigned:
                details += f"\n**Role Assigned:** Yes (account age: {account_age} days)"
            elif member:
                details += f"\n**Role Assigned:** No (account age: {(datetime.now(timezone.utc) - member.created_at).days} days - no role needed)"
            else:
                details += "\n**Role Assigned:** N/A (user not in server yet)"

            logger.info(f"User {username} ({user_id}) added to whitelist by {interaction.user} in guild {guild.name}")

            return {
                'success': True,
                'message': f"Successfully added **{username}** to the whitelist!",
                'details': details
            }

        except Exception as e:
            logger.error(f"Error adding to whitelist: {e}", exc_info=True)
            return {
                'success': False,
                'message': f"An error occurred: {str(e)}"
            }

    @app_commands.command(name="add", description="Add a member to the whitelist (use User ID or exact username)")
    @app_commands.describe(user="The member to whitelist (User ID or exact username - case sensitive)")
    @has_whitelist_permissions_app()
    @app_commands.guild_only()
    async def add(self, interaction: discord.Interaction, user: str):
        """Add a member to the whitelist with reason modal."""
        # First, try to resolve the user to provide better feedback
        resolution = await self._resolve_user_identifier(interaction.guild, user)

        if not resolution:
            embed = discord.Embed(
                title="❌ User Not Found",
                description=f"Could not find user: `{user}`\n\n"
                            "**Tips:**\n"
                            "• Usernames are case-sensitive\n"
                            "• Use the exact username or user ID\n"
                            "• User ID is more reliable than username",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        user_id, username, in_guild = resolution
        member = interaction.guild.get_member(user_id) if in_guild else None

        # Open modal for reason
        modal = WhitelistReasonModal(self, user, resolved_member=member or user_id)
        await interaction.response.send_modal(modal)

    @app_commands.command(name="remove", description="Remove a member from the whitelist")
    @app_commands.describe(user="The member to remove (User ID or exact username - case sensitive)")
    @has_whitelist_permissions_app()
    @app_commands.guild_only()
    async def remove(self, interaction: discord.Interaction, user: str):
        """Remove a member from the whitelist."""
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild

        try:
            # Resolve user
            resolution = await self._resolve_user_identifier(guild, user)
            if not resolution:
                embed = discord.Embed(
                    title="❌ User Not Found",
                    description=f"Could not find user: `{user}`",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            user_id, username, in_guild = resolution
            member = guild.get_member(user_id) if in_guild else None

            # Get whitelist collection
            whitelist_collection = db_manager.get_collection_manager('serverdata_whitelist')

            # Check if whitelisted
            existing = await whitelist_collection.find_one({
                'guild_id': guild.id,
                'user_id': user_id
            })

            if not existing or not existing.get('is_active', True):
                embed = discord.Embed(
                    title="❌ Not Whitelisted",
                    description=f"**{username}** (`{user_id}`) is not on the whitelist.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            # Remove from whitelist (soft delete)
            await whitelist_collection.update_one(
                {'guild_id': guild.id, 'user_id': user_id},
                {'$set': {
                    'is_active': False,
                    'removed_at': datetime.now(timezone.utc),
                    'removed_by': interaction.user.id
                }}
            )

            # Remove role if assigned
            role_removed = False
            if member and existing.get('role_assigned', False):
                try:
                    role = discord.utils.get(guild.roles, name=WHITELIST_ROLE_NAME)
                    if role and role in member.roles:
                        await member.remove_roles(role, reason=f"Removed from whitelist by {interaction.user}")
                        role_removed = True
                        logger.info(f"Removed whitelist role from {member}")
                except Exception as e:
                    logger.error(f"Failed to remove whitelist role: {e}")

            details = f"**User:** {username} (`{user_id}`)"
            if role_removed:
                details += "\n**Role Removed:** Yes"

            embed = discord.Embed(
                title="✅ Removed from Whitelist",
                description=f"Successfully removed **{username}** from the whitelist.",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Details", value=details, inline=False)
            embed.set_footer(text=f"Action by {interaction.user}", icon_url=interaction.user.display_avatar.url)

            logger.info(f"User {username} ({user_id}) removed from whitelist by {interaction.user} in guild {guild.name}")
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error removing from whitelist: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="list", description="List all whitelisted members")
    @has_whitelist_permissions_app()
    @app_commands.guild_only()
    async def list_whitelist(self, interaction: discord.Interaction):
        """List all whitelisted members."""
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild

        try:
            whitelist_collection = db_manager.get_collection_manager('serverdata_whitelist')

            # Get all active whitelist entries for this guild
            entries = await whitelist_collection.find_many(
                {'guild_id': guild.id, 'is_active': True},
                sort=[('added_at', -1)]
            )

            if not entries:
                embed = discord.Embed(
                    title="📋 Whitelist",
                    description="No members are currently whitelisted.",
                    color=discord.Color.blue()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            # Create embed with list
            embed = discord.Embed(
                title=f"📋 Whitelist ({len(entries)} members)",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )

            # Group entries for display (max 25 fields)
            for i, entry in enumerate(entries[:25], 1):
                username = entry.get('username', 'Unknown')
                user_id = entry.get('user_id')
                added_by = entry.get('added_by')
                added_at = entry.get('added_at')
                reason = entry.get('reason', 'No reason provided')
                role_assigned = entry.get('role_assigned', False)

                value = f"**ID:** `{user_id}`\n"
                value += f"**Added by:** <@{added_by}>\n"
                value += f"**Date:** <t:{int(added_at.timestamp())}:R>\n"
                value += f"**Role:** {'✅ Assigned' if role_assigned else '❌ Not assigned'}\n"
                value += f"**Reason:** {reason[:100]}{'...' if len(reason) > 100 else ''}"

                embed.add_field(
                    name=f"{i}. {username}",
                    value=value,
                    inline=False
                )

            if len(entries) > 25:
                embed.set_footer(text=f"Showing 25 of {len(entries)} entries")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error listing whitelist: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="check", description="Check if a member is whitelisted")
    @app_commands.describe(user="The member to check (User ID or exact username - case sensitive)")
    @has_whitelist_permissions_app()
    @app_commands.guild_only()
    async def check(self, interaction: discord.Interaction, user: str):
        """Check if a member is whitelisted."""
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild

        try:
            # Resolve user
            resolution = await self._resolve_user_identifier(guild, user)
            if not resolution:
                embed = discord.Embed(
                    title="❌ User Not Found",
                    description=f"Could not find user: `{user}`",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            user_id, username, in_guild = resolution

            # Get whitelist collection
            whitelist_collection = db_manager.get_collection_manager('serverdata_whitelist')

            # Check whitelist
            entry = await whitelist_collection.find_one({
                'guild_id': guild.id,
                'user_id': user_id
            })

            if not entry or not entry.get('is_active', True):
                embed = discord.Embed(
                    title="❌ Not Whitelisted",
                    description=f"**{username}** (`{user_id}`) is **not** on the whitelist.",
                    color=discord.Color.red()
                )
            else:
                embed = discord.Embed(
                    title="✅ Whitelisted",
                    description=f"**{username}** (`{user_id}`) is on the whitelist.",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )

                embed.add_field(name="Added by", value=f"<@{entry.get('added_by')}>", inline=True)
                embed.add_field(name="Date", value=f"<t:{int(entry.get('added_at').timestamp())}:F>", inline=True)
                embed.add_field(name="Role Assigned", value="✅ Yes" if entry.get('role_assigned', False) else "❌ No", inline=True)
                embed.add_field(name="Reason", value=entry.get('reason', 'No reason provided'), inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error checking whitelist: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Unified error handler for whitelist commands."""
        try:
            if isinstance(error, app_commands.CheckFailure):
                embed = discord.Embed(
                    title="❌ Permission Denied",
                    description="You don't have permission to manage the whitelist.\n"
                                "Required: `Manage Roles` permission or Admin/Moderator/Staff role.",
                    color=discord.Color.red()
                )
                if interaction.response.is_done():
                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, ephemeral=True)

            elif isinstance(error, app_commands.NoPrivateMessage):
                if interaction.response.is_done():
                    await interaction.followup.send("❌ This command can only be used in a server.", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)

            else:
                logger.error(f"Unhandled error in whitelist commands: {error}", exc_info=True)
                embed = discord.Embed(
                    title="❌ Unexpected Error",
                    description="An unexpected error occurred. Please try again later.",
                    color=discord.Color.red()
                )
                if interaction.response.is_done():
                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as inner_e:
            logger.error(f"Error in whitelist cog error handler: {inner_e}", exc_info=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(WhitelistGroup(bot))
