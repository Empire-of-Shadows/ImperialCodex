import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict
from collections import defaultdict
import aiohttp
import discord
from utils.bot import bot, WELCOME_CHANNEL_ID, TOKEN, s
from utils.cache import cache_manager
from Guide.guide import get_help_menu
from utils.logger import get_logger


class WelcomeView(discord.ui.View):
    """Fallback view for welcome message if Components v2 fails"""

    def __init__(self):
        super().__init__(timeout=None)

        # Add guide button
        self.add_item(discord.ui.Button(
            style=discord.ButtonStyle.success,
            label="✅ Guide",
            custom_id="Need Help?"
        ))

        # Add link buttons
        self.add_item(discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="📜 Rules",
            url="https://discord.com/channels/1265120128295632926/1265122523599863930"
        ))

        self.add_item(discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="🗣️Come Chat!",
            url="https://discord.com/channels/1265120128295632926/1265122926823211018"
        ))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Handle button clicks in fallback view"""
        if interaction.data.get("custom_id") == "Need Help?":
            from Guide.guide import get_help_menu
            embed, view = await get_help_menu(interaction.user.id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            return True
        return True


class GuildEventHandler:
    """Handles all guild-related events with enhanced caching and rate limiting"""

    def __init__(self, bot, cache_manager):
        self.bot = bot
        self.cache_manager = cache_manager
        self.logger = get_logger("GuildEventHandler")

        # Enhanced guild-specific rate limiting storage
        self.dm_rate_limits: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            'count': 0,
            'last_reset': datetime.now(timezone.utc),
            'blocked_until': None,
            'total_attempts': 0,
            'first_attempt': None
        })

        # Enhanced guild cache with more comprehensive data
        self.guild_cache: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            'member_count': 0,
            'bot_count': 0,
            'human_count': 0,  # NEW: Track human members separately
            'online_count': 0,
            'new_member_joins_today': 0,
            'kicks_today': 0,
            'last_activity': None,
            'voice_channels_active': 0,
            'recent_messages': 0,
            'moderation_actions': [],
            'member_retention_rate': 0.0,
            'popular_channels': {},
            'role_distribution': {},
            'timezone_distribution': {},
            'join_patterns': {
                'hourly': defaultdict(int),
                'daily': defaultdict(int),
                'weekly': defaultdict(int)
            },
            'security_metrics': {
                'suspicious_joins': 0,
                'account_age_violations': 0,
                'rapid_joins': 0
            }
        })

        # Rate limit configuration - can be guild-specific
        self.rate_limits = {
            'new_account_days': 30,
            'max_dms_per_hour': 2,
            'max_dms_per_day': 5,
            'block_duration_hours': 24,
        }

    async def _count_human_members(self, guild: discord.Guild) -> int:
        """Count only human (non-bot) members in the guild"""
        return sum(1 for member in guild.members if not member.bot)

    async def initialize_guild_cache(self, guild: discord.Guild):
        """Initialize comprehensive guild cache data with human member tracking"""
        try:
            self.logger.info(f"Initializing comprehensive guild cache for {guild.name} ({guild.id})")

            cache_data = self.guild_cache[guild.id]

            # Basic guild metrics with human member tracking
            cache_data['member_count'] = guild.member_count
            cache_data['bot_count'] = sum(1 for member in guild.members if member.bot)
            cache_data['human_count'] = await self._count_human_members(guild)  # NEW: Track humans separately
            cache_data['online_count'] = sum(1 for member in guild.members
                                             if hasattr(member, 'status') and member.status != discord.Status.offline)

            # Voice channel activity
            cache_data['voice_channels_active'] = sum(1 for vc in guild.voice_channels if vc.members)

            # Role distribution - only for human members
            role_dist = defaultdict(int)
            for member in guild.members:
                if not member.bot:  # NEW: Only count human members for role distribution
                    for role in member.roles:
                        if not role.is_default():
                            role_dist[role.name] += 1
            cache_data['role_distribution'] = dict(role_dist)

            # Channel activity estimation (simplified)
            channel_activity = {}
            for channel in guild.text_channels:
                try:
                    # Get recent message count (last 24 hours)
                    recent_count = 0
                    async for message in channel.history(after=datetime.now(timezone.utc) - timedelta(hours=24),
                                                         limit=100):
                        recent_count += 1
                    channel_activity[channel.name] = recent_count
                except (discord.Forbidden, discord.HTTPException):
                    channel_activity[channel.name] = 0

            cache_data['popular_channels'] = dict(sorted(channel_activity.items(),
                                                         key=lambda x: x[1], reverse=True)[:5])

            # Initialize today's counters
            cache_data['new_member_joins_today'] = 0
            cache_data['kicks_today'] = 0
            cache_data['last_activity'] = datetime.now(timezone.utc).isoformat()

            # Security metrics initialization
            cache_data['security_metrics']['suspicious_joins'] = 0
            cache_data['security_metrics']['account_age_violations'] = 0
            cache_data['security_metrics']['rapid_joins'] = 0

            self.logger.info(f"Guild cache initialized: {guild.member_count} members, "
                             f"{cache_data['bot_count']} bots, {cache_data['human_count']} humans, "
                             f"{cache_data['online_count']} online")

        except Exception as e:
            self.logger.error(f"Error initializing guild cache for {guild.name}: {e}")

    async def update_guild_metrics(self, guild: discord.Guild, event_type: str, **kwargs):
        """Update guild metrics based on events with human member tracking"""
        try:
            cache_data = self.guild_cache[guild.id]
            now = datetime.now(timezone.utc)

            if event_type == "member_join":
                member = kwargs.get('member')
                if member and not member.bot:  # NEW: Only count human joins
                    cache_data['new_member_joins_today'] += 1
                    cache_data['human_count'] = await self._count_human_members(guild)  # Update human count

                    # Track join patterns only for humans
                    hour = now.hour
                    day = now.strftime('%Y-%m-%d')
                    week = now.strftime('%Y-W%U')

                    cache_data['join_patterns']['hourly'][hour] += 1
                    cache_data['join_patterns']['daily'][day] += 1
                    cache_data['join_patterns']['weekly'][week] += 1

                    # Check for rapid joins (security metric)
                    recent_joins = cache_data['join_patterns']['hourly'][hour]
                    if recent_joins > 10:  # More than 10 joins in an hour
                        cache_data['security_metrics']['rapid_joins'] += 1

                    if member:
                        account_age = now - member.created_at
                        if account_age.days < self.rate_limits['new_account_days']:
                            cache_data['security_metrics']['account_age_violations'] += 1

                # Always update total member count
                cache_data['member_count'] = guild.member_count
                cache_data['bot_count'] = sum(1 for m in guild.members if m.bot)

            elif event_type == "member_remove":
                member = kwargs.get('member')
                if member and not member.bot:  # NEW: Only update human count for human removals
                    cache_data['human_count'] = await self._count_human_members(guild)

                cache_data['member_count'] = guild.member_count
                cache_data['bot_count'] = sum(1 for m in guild.members if m.bot)

            elif event_type == "member_kick":
                member = kwargs.get('member')
                if member and not member.bot:  # NEW: Only count human kicks
                    cache_data['kicks_today'] += 1

            cache_data['last_activity'] = now.isoformat()

            # Update database cache periodically
            await self.cache_manager.cache_guild_info(guild)

        except Exception as e:
            self.logger.error(f"Error updating guild metrics for {guild.name}: {e}")

    async def get_guild_analytics(self, guild_id: int) -> Dict[str, Any]:
        """Get comprehensive guild analytics with accurate human counts"""
        cache_data = self.guild_cache.get(guild_id, {})

        analytics = {
            'basic_stats': {
                'total_members': cache_data.get('member_count', 0),
                'bot_count': cache_data.get('bot_count', 0),
                'human_members': cache_data.get('human_count', 0),  # Use pre-calculated human count
                'online_members': cache_data.get('online_count', 0)
            },
            'activity_stats': {
                'joins_today': cache_data.get('new_member_joins_today', 0),
                'kicks_today': cache_data.get('kicks_today', 0),
                'active_voice_channels': cache_data.get('voice_channels_active', 0),
                'popular_channels': cache_data.get('popular_channels', {})
            },
            'security_metrics': cache_data.get('security_metrics', {}),
            'join_patterns': cache_data.get('join_patterns', {}),
            'role_distribution': cache_data.get('role_distribution', {}),
            'last_updated': cache_data.get('last_activity')
        }

        return analytics

    async def can_send_dm(self, member: discord.Member) -> tuple[bool, str]:
        """Enhanced DM rate limiting with guild-specific tracking"""
        # NEW: Skip rate limiting for bots entirely
        if member.bot:
            return False, "Bots cannot receive DMs"

        now = datetime.now(timezone.utc)
        account_age = now - member.created_at

        # Only apply rate limits to new accounts
        if account_age.days >= self.rate_limits['new_account_days']:
            return True, "Account old enough"

        user_limits = self.dm_rate_limits[member.id]

        # Initialize first attempt tracking
        if user_limits['first_attempt'] is None:
            user_limits['first_attempt'] = now

        user_limits['total_attempts'] += 1

        # Check if user is currently blocked
        if user_limits.get('blocked_until') and now < user_limits['blocked_until']:
            remaining = user_limits['blocked_until'] - now
            return False, f"Blocked for {remaining.seconds // 3600}h {(remaining.seconds % 3600) // 60}m"

        # Reset counters if needed (hourly reset)
        if now - user_limits['last_reset'] >= timedelta(hours=1):
            user_limits['count'] = 0
            user_limits['last_reset'] = now
            user_limits['blocked_until'] = None

        # Check hourly limit
        if user_limits['count'] >= self.rate_limits['max_dms_per_hour']:
            # Block the user
            user_limits['blocked_until'] = now + timedelta(hours=self.rate_limits['block_duration_hours'])
            self.logger.warning(
                f"User {member} ({member.id}) hit DM rate limit, blocked for {self.rate_limits['block_duration_hours']} hours"
            )
            return False, "Rate limit exceeded"

        return True, "Within limits"

    async def record_dm_sent(self, member: discord.Member):
        """Record that a DM was sent with enhanced tracking"""
        # NEW: Don't record DMs for bots
        if member.bot:
            return

        user_limits = self.dm_rate_limits[member.id]
        user_limits['count'] += 1

        # Update guild security metrics
        await self.update_guild_metrics(
            member.guild,
            "dm_sent",
            member=member,
            reason="account_age_restriction"
        )

        self.logger.info(f"DM count for {member} ({member.id}): {user_limits['count']}")

    async def handle_member_join(self, member: discord.Member):
        """Handle member join with bot filtering and comprehensive tracking"""
        self.logger.info(f"\n{s}New member joined: {member} ({member.id}) in {member.guild.name}\n")

        # NEW: Skip processing for bot accounts entirely
        if member.bot:
            self.logger.info(f"\n{s}Skipping bot account: {member} ({member.id})\n")
            return

        now = datetime.now(timezone.utc)
        account_age = now - member.created_at
        guild = member.guild

        # Update guild metrics (will only count humans due to our update_guild_metrics changes)
        await self.update_guild_metrics(guild, "member_join", member=member)

        # Initialize guild cache if needed
        if guild.id not in self.guild_cache:
            await self.initialize_guild_cache(guild)

        # Default avatar fallback
        avatar_url = member.display_avatar.url if member.display_avatar else "https://cdn.discordapp.com/embed/avatars/0.png"

        if account_age.days < 90:
            # Check if user is whitelisted before kicking
            from Database.DatabaseManager import db_manager
            try:
                whitelist_collection = db_manager.get_collection_manager('serverdata_whitelist')
                whitelist_entry = await whitelist_collection.find_one({
                    'guild_id': guild.id,
                    'user_id': member.id,
                    'is_active': True
                })

                if whitelist_entry:
                    # User is whitelisted - allow them to join and assign role
                    self.logger.info(f"\n{s}Member {member} is whitelisted, bypassing age restriction (account age: {account_age.days} days)\n")

                    # Assign whitelist role if not already assigned
                    if not whitelist_entry.get('role_assigned', False):
                        try:
                            from NewMembers.admin.whitelist import WHITELIST_ROLE_NAME, WHITELIST_ROLE_COLOR
                            role = discord.utils.get(guild.roles, name=WHITELIST_ROLE_NAME)
                            if not role:
                                # Create role if it doesn't exist
                                role = await guild.create_role(
                                    name=WHITELIST_ROLE_NAME,
                                    color=WHITELIST_ROLE_COLOR,
                                    reason="Whitelist role for new members with new accounts",
                                    mentionable=False,
                                    hoist=True
                                )
                            await member.add_roles(role, reason="Whitelisted new member")

                            # Update database
                            await whitelist_collection.update_one(
                                {'guild_id': guild.id, 'user_id': member.id},
                                {'$set': {
                                    'role_assigned': True,
                                    'role_assigned_at': datetime.now(timezone.utc),
                                    'account_age_at_join': account_age.days
                                }}
                            )
                            self.logger.info(f"\n{s}Assigned whitelist role to {member}\n")
                        except Exception as role_error:
                            self.logger.error(f"\n{s}Failed to assign whitelist role to {member}: {role_error}\n")

                    # Continue with normal welcome flow (skip the kick)
                    channel = member.guild.get_channel(WELCOME_CHANNEL_ID)
                    if channel:
                        try:
                            # Update members cache for this guild
                            await self.cache_manager.cache_members(member.guild)
                            self.logger.info(f"\n{s}Member cache updated for {member.guild.name}\n")
                        except Exception as e:
                            self.logger.error(f"\n{s}Error updating member cache for {member.guild.name}: {e}\n")

                        try:
                            await asyncio.sleep(1.2)
                            await self.send_welcome_message(member)
                            self.logger.info(f"Interactive welcome message sent for whitelisted member {member}\n")
                        except Exception as e:
                            self.logger.error(f"Error sending welcome message: {e}\n")
                    return  # Exit early, don't kick
            except Exception as whitelist_error:
                self.logger.error(f"\n{s}Error checking whitelist: {whitelist_error}\n", exc_info=True)
                # Continue with normal flow if whitelist check fails

            # Account is too new — check if we can send DM with rate limiting
            can_dm, reason = await self.can_send_dm(member)

            if can_dm:
                try:
                    await member.send(
                        f"Hey {member.name}! 👋\n\n"
                        f"Unfortunately, your Discord account is too new to join our server (created {account_age.days} days ago).\n"
                        f"We require accounts to be a certain number of days old to help prevent spam and protect our community.\n\n"
                        f"You're welcome to try again once your account is older. Thanks for understanding! 🙏"
                    )
                    await self.record_dm_sent(member)
                    self.logger.info(f"\n{s}Sent DM to {member} about new account restriction.\n")
                except discord.Forbidden:
                    self.logger.warning(f"\n{s}Could not DM {member} before kick (Forbidden).\n")
                except Exception as e:
                    self.logger.error(f"\n{s}Failed to DM {member}: {e}")
            else:
                self.logger.info(f"\n{s}Skipped DM to {member} due to rate limiting: {reason}\n")

            try:
                await asyncio.sleep(1.2)
                await member.kick(reason=f"Account too new ({account_age.days} days old)")
                await self.update_guild_metrics(guild, "member_kick", member=member)
                self.logger.info(f"\n{s}Kicked {member} due to account age ({account_age.days} days).\n")
            except Exception as e:
                self.logger.error(f"\n{s}Failed to kick {member}: {e}\n")

            await asyncio.sleep(1.2)
            return

        # Account is old enough - proceed with welcome
        channel = member.guild.get_channel(WELCOME_CHANNEL_ID)
        if channel:
            try:
                # Update members cache for this guild
                await self.cache_manager.cache_members(member.guild)
                self.logger.info(f"\n{s}Member cache updated for {member.guild.name}\n")
            except Exception as e:
                self.logger.error(f"\n{s}Error updating member cache for {member.guild.name}: {e}\n")

            try:
                await asyncio.sleep(1.2)
                await self.send_welcome_message(member)
                self.logger.info(f"Interactive welcome message sent for {member}\n")
            except Exception as e:
                self.logger.error(f"Error sending welcome message: {e}\n")

    async def send_welcome_message(self, member: discord.Member, avatar_url: str = None):
        """Send enhanced welcome message using Discord components v2 through discord.py"""
        channel = member.guild.get_channel(WELCOME_CHANNEL_ID)
        if not channel:
            self.logger.error(f"Welcome channel {WELCOME_CHANNEL_ID} not found")
            return

        if avatar_url is None:
            avatar_url = member.display_avatar.url if member.display_avatar else "https://cdn.discordapp.com/embed/avatars/0.png"

        try:
            # Get guild analytics for personalized welcome
            analytics = await self.get_guild_analytics(member.guild.id)
            member_number = analytics['basic_stats']['human_members']  # This now uses accurate human count

            # Create the layout view using discord.py components v2
            layout_view = discord.ui.LayoutView()

            # Add separator
            layout_view.add_item(discord.ui.Separator())

            # Create welcome header section with thumbnail accessory
            header_accessory = discord.ui.Thumbnail(media=avatar_url, description="Your avatar")
            header_section = discord.ui.Section(accessory=header_accessory)
            header_section.add_item(discord.ui.TextDisplay(
                f"# Welcome to the server, <@{member.id}>!\n*You're member #{member_number}!*"
            ))

            # Add action buttons row
            button_row = discord.ui.ActionRow()
            guide_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label="✅ Guide",
                custom_id="Need Help?"

            )
            rules_button = discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="📜 Rules",
                url="https://discord.com/channels/1265120128295632926/1265122523599863930"
            )
            chat_button = discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="🗣️Come Chat!",
                url="https://discord.com/channels/1265120128295632926/1265122926823211018"
            )

            # Put buttons in their own container, not in the section
            button_row = discord.ui.ActionRow()
            button_row.add_item(guide_button)
            button_row.add_item(rules_button)
            button_row.add_item(chat_button)

            button_container = discord.ui.Container()
            button_container.add_item(button_row)

            # Add both separately to the layout
            layout_view.add_item(header_section)
            layout_view.add_item(button_container)

            # Add separator
            layout_view.add_item(discord.ui.Separator())

            # Add welcome content section with button accessory
            welcome_accessory = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="🏠 Server Info",
                emoji="ℹ️",
                custom_id="server_info_welcome"
            )
            welcome_section = discord.ui.Section(accessory=welcome_accessory)
            welcome_text = (
                "**Explore and have fun!**\n"
                "- Play games like UNO, TicTacToe, Hangman\n"
                "- Compete in leaderboards\n"
                f"- Join voice chat and events 🎤 ({analytics['activity_stats']['active_voice_channels']} active now!)"
            )
            welcome_section.add_item(discord.ui.TextDisplay(content=welcome_text))
            layout_view.add_item(welcome_section)

            layout_view.add_item(discord.ui.Separator())  # optional bottom spacer
            # Add channels section with button accessory
            channels_accessory = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="🎮 All Channels",
                emoji="📋",
                custom_id="channels_info_welcome"
            )
            channels_section = discord.ui.Section(accessory=channels_accessory)
            channels_section.add_item(discord.ui.TextDisplay("🎮 Some other channels you might like"))

            # Add channel buttons row
            media_button = discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="📷 Media",
                url="https://discord.com/channels/1265120128295632926/1265122765279727657"
            )
            clips_button = discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="🎮 Game Clips",
                url="https://discord.com/channels/1265120128295632926/1265123462284836935"
            )
            gamer_chat_button = discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="💬 Gamer Chat",
                url="https://discord.com/channels/1265120128295632926/1265123424892616705"
            )

            channel_row = discord.ui.ActionRow()
            channel_row.add_item(media_button)
            channel_row.add_item(clips_button)
            channel_row.add_item(gamer_chat_button)

            channel_container = discord.ui.Container()
            channel_container.add_item(channel_row)

            layout_view.add_item(channels_section)
            layout_view.add_item(channel_container)

            # Add separator
            layout_view.add_item(discord.ui.Separator())

            # Add final content section with button accessory
            final_accessory = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="🎯 Get Started",
                emoji="🚀",
                custom_id="get_started_welcome"
            )
            final_section = discord.ui.Section(accessory=final_accessory)
            final_text = (
                "**Explore and have fun!**\n"
                "- Play games like UNO, TicTacToe, Hangman\n"
                "- Compete in leaderboards\n"
                f"- Join voice chat and events 🎤 ({analytics['activity_stats']['active_voice_channels']} active now!)"
            )
            final_section.add_item(discord.ui.TextDisplay(final_text))
            layout_view.add_item(final_section)

            # Set overall layout accent color
            layout_view.accent_color = 0x5865F2

            # Send the message using discord.py
            await channel.send(view=layout_view)

            self.logger.info(f"\n{s}[WELCOME] Successfully sent Components v2 welcome message for {member}\n")

        except Exception as e:
            self.logger.error(f"Error sending Components v2 welcome message: {e}")
            # Fallback to simple embed if layout fails
            try:
                embed = discord.Embed(
                    title=f"🌟 Welcome {member.display_name}!",
                    description=f"Welcome to the server! You're member #{analytics['basic_stats']['human_members']}!",
                    color=0x5865F2
                )
                embed.set_thumbnail(url=avatar_url)

                # Create simple view with buttons
                view = WelcomeView()
                await channel.send(embed=embed, view=view)

                self.logger.info(f"\n{s}[WELCOME] Sent fallback embed welcome for {member}\n")
            except Exception as fallback_error:
                self.logger.error(f"Fallback welcome message also failed: {fallback_error}")

    async def handle_interaction(self, interaction: discord.Interaction):
        """Handle button interactions"""
        author_id = interaction.user.id
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get("custom_id")

            if custom_id == "Need Help?":
                from Guide.guide import get_help_menu
                embed, view = await get_help_menu(author_id)
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

            elif custom_id == "server_info_welcome":
                # Handle the server info button from welcome message
                embed = discord.Embed(
                    title=f"📊 {interaction.guild.name} Server Information",
                    color=discord.Color.blue()
                )

                if interaction.guild.icon:
                    embed.set_thumbnail(url=interaction.guild.icon.url)

                # Basic stats
                embed.add_field(
                    name="📈 Statistics",
                    value=(
                        f"**Members:** {interaction.guild.member_count:,}\n"
                        f"**Created:** <t:{int(interaction.guild.created_at.timestamp())}:F>\n"
                        f"**Server ID:** {interaction.guild.id}"
                    ),
                    inline=False
                )

                # Channel info
                text_channels = len([c for c in interaction.guild.channels if isinstance(c, discord.TextChannel)])
                voice_channels = len([c for c in interaction.guild.channels if isinstance(c, discord.VoiceChannel)])

                embed.add_field(
                    name="📋 Channels",
                    value=f"**Text:** {text_channels}\n**Voice:** {voice_channels}",
                    inline=True
                )

                # Roles
                role_count = len(interaction.guild.roles) - 1  # Exclude @everyone
                embed.add_field(
                    name="🏷️ Roles",
                    value=f"**Total:** {role_count}",
                    inline=True
                )

                # Boost info
                if interaction.guild.premium_tier > 0:
                    embed.add_field(
                        name="🚀 Nitro Boost",
                        value=f"**Level {interaction.guild.premium_tier}**\n**Boosts:** {interaction.guild.premium_subscription_count}",
                        inline=True
                    )

                embed.set_footer(text="Welcome to our community!")
                embed.timestamp = discord.utils.utcnow()

                await interaction.response.send_message(embed=embed, ephemeral=True)

            elif custom_id == "channels_info_welcome":
                # Handle the channels info button
                embed = discord.Embed(
                    title=f"📋 {interaction.guild.name} - All Channels",
                    color=discord.Color.green()
                )

                # Get channel categories and organize them
                categories = interaction.guild.categories
                text_channels = [c for c in interaction.guild.text_channels if c.category is None]
                voice_channels = [c for c in interaction.guild.voice_channels if c.category is None]

                if categories:
                    category_info = []
                    for category in categories[:10]:  # Limit to prevent overflow
                        cat_channels = len(category.channels)
                        category_info.append(f"📁 **{category.name}** ({cat_channels} channels)")

                    embed.add_field(
                        name="📂 Channel Categories",
                        value="\n".join(category_info) or "None",
                        inline=False
                    )

                if text_channels:
                    embed.add_field(
                        name="💬 Uncategorized Text Channels",
                        value=f"{len(text_channels)} channels",
                        inline=True
                    )

                if voice_channels:
                    embed.add_field(
                        name="🔊 Uncategorized Voice Channels",
                        value=f"{len(voice_channels)} channels",
                        inline=True
                    )

                embed.add_field(
                    name="📊 Channel Summary",
                    value=(
                        f"**Total Categories:** {len(categories)}\n"
                        f"**Total Text Channels:** {len([c for c in interaction.guild.channels if isinstance(c, discord.TextChannel)])}\n"
                        f"**Total Voice Channels:** {len([c for c in interaction.guild.channels if isinstance(c, discord.VoiceChannel)])}"
                    ),
                    inline=False
                )

                embed.set_footer(text="Explore all our channels!")
                await interaction.response.send_message(embed=embed, ephemeral=True)

            elif custom_id == "get_started_welcome":
                # Handle the get started button
                embed = discord.Embed(
                    title="🚀 Getting Started Guide",
                    description="Here's how to make the most of your time in our server!",
                    color=discord.Color.gold()
                )

                embed.add_field(
                    name="🎮 Gaming Features",
                    value=(
                        "• Use `/uno` to start a UNO game\n"
                        "• Try `/tictactoe` for quick matches\n"
                        "• Play `/hangman` with friends\n"
                        "• Check leaderboards with `/stats`"
                    ),
                    inline=False
                )

                embed.add_field(
                    name="💬 Community Features",
                    value=(
                        "• Share suggestions with `/suggest`\n"
                        "• Get help by mentioning our bot\n"
                        "• Join voice channels for events\n"
                        "• Participate in community discussions"
                    ),
                    inline=False
                )

                embed.add_field(
                    name="🎯 Quick Tips",
                    value=(
                        "• Read the rules in <#1265122523599863930>\n"
                        "• Introduce yourself in chat\n"
                        "• Join voice channels when active\n"
                        "• Have fun and be respectful!"
                    ),
                    inline=False
                )

                embed.set_footer(text="Welcome to the community! 🎉")
                await interaction.response.send_message(embed=embed, ephemeral=True)

    async def handle_member_remove(self, member: discord.Member):
        """Handle member removal with enhanced tracking and cache cleanup"""
        self.logger.info(f"\n{s}Member left: {member.name} ({member.id}) in guild: {member.guild.name}\n")

        # Update guild metrics (handles human/bot distinction internally)
        await self.update_guild_metrics(member.guild, "member_remove", member=member)

        # NEW: Clean up rate limit data for users who leave (humans only)
        if not member.bot and member.id in self.dm_rate_limits:
            del self.dm_rate_limits[member.id]
            self.logger.info(f"Cleaned up rate limit data for {member.id}")

        try:
            # Update members cache for this guild
            await self.cache_manager.cache_members(member.guild)
            self.logger.info(f"\n{s}Member cache updated for {member.guild.name}\n")
        except Exception as e:
            self.logger.error(f"\n{s}Error updating member cache for {member.guild.name}: {e}\n")

    async def handle_guild_role_update(self, before: discord.Role, after: discord.Role):
        """Handle role updates with caching"""
        self.logger.info(f"\n{s}Role updated: {after.name} ({after.id}) in guild: {after.guild.name}\n")

        # Update guild role distribution cache - only for human members
        guild_data = self.guild_cache[after.guild.id]
        role_dist = defaultdict(int)
        for member in after.guild.members:
            if not member.bot:  # NEW: Only count human members for role distribution
                for role in member.roles:
                    if not role.is_default():
                        role_dist[role.name] += 1
        guild_data['role_distribution'] = dict(role_dist)

        try:
            # Update roles cache for this guild
            await self.cache_manager.cache_roles(after.guild)
            self.logger.info(f"\n{s}Roles cache updated for {after.guild.name}\n")
        except Exception as e:
            self.logger.error(f"Error updating roles cache for {after.guild.name}: {e}\n")

    async def handle_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        """Handle channel updates with caching"""
        self.logger.info(f"\n{s}Channel updated: {after.name} ({after.id}) in guild: {after.guild.name}\n")
        try:
            # Update channels cache for this guild
            await self.cache_manager.cache_channels(after.guild)
            self.logger.info(f"\n{s}Channels cache updated for {after.guild.name}\n")
        except Exception as e:
            self.logger.error(f"\n{s}Error updating channels cache for {after.guild.name}: {e}\n")


# Create the guild event handler instance
guild_handler = GuildEventHandler(bot, cache_manager)


# Keep your existing event handlers but delegate to the class

# Section: Interactions
# Missing in this section:
# - (None, all handled via on_interaction)
@bot.event
async def on_interaction(interaction: discord.Interaction):
    await guild_handler.handle_interaction(interaction)

# Section: Member lifecycle and moderation
# Missing in this section:
# - on_member_chunk
@bot.event
async def on_member_join(member):
    await guild_handler.handle_member_join(member)

@bot.event
async def on_member_remove(member: discord.Member):
    await guild_handler.handle_member_remove(member)

@bot.event
async def on_member_update(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_member_update - {before.id} in {getattr(before, 'guild', 'N/A')}")
    pass

@bot.event
async def on_member_ban(guild, user):
    #ToDO
    guild_handler.logger.info(f"Event: on_member_ban - {user} in {guild.name} ({guild.id})")
    pass

@bot.event
async def on_member_unban(guild, user):
    #ToDO
    guild_handler.logger.info(f"Event: on_member_unban - {user} in {guild.name} ({guild.id})")
    pass

# Section: Connection / Lifecycle
# Missing in this section:
# - on_ready
# - on_shard_connect
# - on_shard_disconnect
# - on_shard_ready
# - on_shard_resumed

@bot.event
async def on_connect():
    #ToDO
    guild_handler.logger.info("Event: on_connect - connected to gateway")
    pass

@bot.event
async def on_disconnect():
    #ToDO
    guild_handler.logger.info("Event: on_disconnect - disconnected from gateway")
    pass

@bot.event
async def on_resumed():
    #ToDO
    guild_handler.logger.info("Event: on_resumed - session resumed")
    pass

# Section: Guild lifecycle and cache
# Missing in this section:
# - on_guild_integrations_update
# - on_audit_log_entry_create

@bot.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    await guild_handler.handle_guild_role_update(before, after)

@bot.event
async def on_guild_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
    await guild_handler.handle_guild_channel_update(before, after)

@bot.event
async def on_guild_join(guild):
    guild_handler.logger.info(f"Event: on_guild_join - joined guild {guild.name} ({guild.id})")
    # Optionally initialize cache for new guild
    try:
        await guild_handler.initialize_guild_cache(guild)
    except Exception as e:
        guild_handler.logger.error(f"Error initializing cache on guild join: {e}")
    pass

@bot.event
async def on_guild_remove(guild):
    guild_handler.logger.info(f"Event: on_guild_remove - removed from guild {guild.name} ({guild.id})")
    # Clean up guild cache if present
    if guild.id in guild_handler.guild_cache:
        del guild_handler.guild_cache[guild.id]
        guild_handler.logger.info(f"Cleared cache for guild {guild.id}")
    pass

@bot.event
async def on_guild_update(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_guild_update - {after.name} ({after.id})")
    pass

@bot.event
async def on_guild_available(guild):
    #ToDO
    guild_handler.logger.info(f"Event: on_guild_available - {guild.name} ({guild.id})")
    pass

@bot.event
async def on_guild_unavailable(guild):
    #ToDO
    guild_handler.logger.info(f"Event: on_guild_unavailable - {guild.name} ({guild.id})")
    pass

# Section: Roles
# Missing in this section:
# - (None)
@bot.event
async def on_guild_role_create(role):
    guild_handler.logger.info(f"Event: on_guild_role_create - {role.name} ({role.id}) in {role.guild.name}")
    # update role distribution cache
    try:
        await guild_handler.handle_guild_role_update(role, role)
    except Exception:
        # fallback to cache refresh
        await guild_handler.cache_manager.cache_roles(role.guild)
    pass

@bot.event
async def on_guild_role_delete(role):
    guild_handler.logger.info(f"Event: on_guild_role_delete - {role.name} ({role.id}) in {role.guild.name}")
    # refresh roles cache
    try:
        await guild_handler.cache_manager.cache_roles(role.guild)
    except Exception as e:
        guild_handler.logger.error(f"Error updating roles cache after delete: {e}")
    pass

# Section: Emojis and Stickers
# Missing in this section:
# - on_guild_stickers_update
@bot.event
async def on_guild_emojis_update(guild, before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_guild_emojis_update - {guild.name} ({guild.id})")
    pass

# Section: Webhooks and Integrations
# Missing in this section:
# - on_integration_create
# - on_integration_update
@bot.event
async def on_webhooks_update(channel):
    #ToDO
    guild_handler.logger.info(f"Event: on_webhooks_update - channel {getattr(channel, 'name', channel.id)}")
    pass

# Section: Channels
# Missing in this section:
# - on_guild_channel_pins_update
# - on_private_channel_create
# - on_private_channel_delete
# - on_private_channel_update
# - on_private_channel_pins_update
@bot.event
async def on_channel_create(channel):
    #ToDO
    guild_handler.logger.info(f"Event: on_channel_create - {getattr(channel, 'name', channel.id)} in {getattr(channel, 'guild', 'DM')}")
    pass

@bot.event
async def on_channel_delete(channel):
    #ToDO
    guild_handler.logger.info(f"Event: on_channel_delete - {getattr(channel, 'name', channel.id)}")
    pass

# Section: Threads
# Missing in this section:
# - on_thread_member_join
# - on_thread_member_remove
@bot.event
async def on_thread_create(thread):
    #ToDO
    guild_handler.logger.info(f"Event: on_thread_create - {thread.name} ({thread.id})")
    pass

@bot.event
async def on_thread_update(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_thread_update - {after.name} ({after.id})")
    pass

@bot.event
async def on_thread_delete(thread):
    #ToDO
    guild_handler.logger.info(f"Event: on_thread_delete - {thread.name} ({thread.id})")
    pass

# Section: Voice and Presence
# Missing in this section:
# - (None)
@bot.event
async def on_voice_state_update(member, before, after):
    guild_handler.logger.info(f"Event: on_voice_state_update - {member} in {member.guild.name}")
    # update voice channel counts in cache if present
    try:
        await guild_handler.update_guild_metrics(member.guild, "voice_state_change", member=member)
    except Exception:
        pass
    pass

@bot.event
async def on_presence_update(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_presence_update - {getattr(after, 'id', 'N/A')}")
    pass

# Section: Users and Typing
# Missing in this section:
# - (None)
@bot.event
async def on_user_update(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_user_update - {after.id}")
    pass

@bot.event
async def on_typing(channel, user, when):
    #ToDO
    guild_handler.logger.info(f"Event: on_typing - {user} in {getattr(channel, 'name', channel.id)} at {when}")
    pass

# Section: Messages
# Missing in this section:
# - on_bulk_message_delete
# - on_raw_message_edit
# - on_raw_bulk_message_delete
@bot.event
async def on_message(message):
    #ToDO
    # Ignore bot messages
    if message.author.bot:
        return
    guild_handler.logger.info(f"Event: on_message - {message.author} in {getattr(message.guild, 'name', 'DM')}")
    # Keep default processing intact (commands, etc.)
    await bot.process_commands(message)

@bot.event
async def on_message_edit(before, after):
    #ToDO
    guild_handler.logger.info(f"Event: on_message_edit - message {before.id}")
    pass

@bot.event
async def on_message_delete(message):
    #ToDO
    guild_handler.logger.info(f"Event: on_message_delete - message {getattr(message, 'id', 'raw')}")
    pass

@bot.event
async def on_raw_message_delete(payload):
    #ToDO
    guild_handler.logger.info(f"Event: on_raw_message_delete - id {payload.message_id}")
    pass

@bot.event
async def on_message_delete_bulk(messages):
    #ToDO
    guild_handler.logger.info(f"Event: on_message_delete_bulk - {len(messages)} messages")
    pass

# Section: Reactions
# Missing in this section:
# - on_reaction_clear_emoji
# - on_raw_reaction_clear
# - on_raw_reaction_clear_emoji
@bot.event
async def on_reaction_add(reaction, user):
    #ToDO
    guild_handler.logger.info(f"Event: on_reaction_add - {user} reacted in {getattr(reaction.message, 'id', 'N/A')}")
    pass

@bot.event
async def on_reaction_remove(reaction, user):
    #ToDO
    guild_handler.logger.info(f"Event: on_reaction_remove - {user} removed reaction")
    pass

@bot.event
async def on_reaction_clear(message, reactions):
    #ToDO
    guild_handler.logger.info(f"Event: on_reaction_clear - cleared on message {getattr(message, 'id', 'N/A')}")
    pass

@bot.event
async def on_raw_reaction_add(payload):
    #ToDO
    guild_handler.logger.info(f"Event: on_raw_reaction_add - {payload}")
    pass

@bot.event
async def on_raw_reaction_remove(payload):
    #ToDO
    guild_handler.logger.info(f"Event: on_raw_reaction_remove - {payload}")
    pass

# Section: Invites
# Missing in this section:
# - (None)
@bot.event
async def on_invite_create(invite):
    #ToDO
    guild_handler.logger.info(f"Event: on_invite_create - {invite.code} to {invite.guild}")
    pass

@bot.event
async def on_invite_delete(invite):
    #ToDO
    guild_handler.logger.info(f"Event: on_invite_delete - invite deleted")
    pass