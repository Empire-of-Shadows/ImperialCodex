import discord
from discord.ext import commands
import logging

from configuration.config_system import config
from utils.logger import get_logger

logger = get_logger("Announcements")


class AnnouncementThreadCog(commands.Cog):
    def __init__(self, bot):
        logger.info("Initializing AnnouncementThreadCog")
        self.bot = bot

        # Use the properties you defined in the config
        self.announcement_channel_id = config.announcement_channel_id
        self.thread_auto_create = config.thread_auto_create
        self.thread_name_format = config.thread_name_format
        self.thread_auto_archive_duration = config.thread_auto_archive_duration
        self.thread_welcome_message = config.thread_welcome_message
        self.auto_delete_threads = getattr(config, 'auto_delete_threads', True)  # This one might not exist yet

        # Optional: Log channel names for debugging
        if self.announcement_channel_id:
            channel_name = config.get_channel_name(self.announcement_channel_id)
            logger.debug(f"Announcement channel: {self.announcement_channel_id} ({channel_name or 'name unknown'})")

        logger.info(
            f"AnnouncementThreadCog initialized: channel_id={self.announcement_channel_id}, enabled={self.thread_auto_create}")

    @commands.Cog.listener()
    async def on_message(self, message):
        """Listen for messages in the announcement channel and create threads automatically"""

        logger.info(
            f"Message received in channel {message.channel.id} - Author: {message.author} - Bot: {message.author.bot}")

        # Ignore messages from bots to prevent loops
        if message.author.bot:
            logger.debug("Ignoring bot message")
            return

        # Check if this is the announcement channel
        if not self.announcement_channel_id:
            logger.warning("No announcement channel ID configured")
            return

        if message.channel.id != self.announcement_channel_id:
            logger.debug(
                f"Message not in announcement channel. Expected: {self.announcement_channel_id}, Got: {message.channel.id}")
            return

        logger.info(f"Processing message in announcement channel: {message.content[:50]}...")

        # Check if auto-create is enabled
        if not self.thread_auto_create:
            logger.info("Thread auto-create is disabled")
            return

        # Check if message already has a thread
        has_existing_thread = False

        # Method 1: Check message flags
        if hasattr(message, 'flags') and message.flags.has_thread:
            logger.info("Message has thread flag set to True - skipping thread creation")
            has_existing_thread = True

        # Method 2: Check if thread already exists by trying to fetch it
        try:
            if message.id:
                # Try to get the thread associated with this message
                thread = message.guild.get_thread(message.id)
                if thread:
                    logger.info(f"Thread already exists for message: {thread.name}")
                    has_existing_thread = True
        except Exception as e:
            logger.debug(f"Error checking for existing thread: {e}")

        if has_existing_thread:
            logger.info("Skipping thread creation - thread already exists")
            return

        logger.info("No existing thread found - creating new thread")

        try:
            # Format thread name
            thread_name = self.format_thread_name(message)
            logger.info(f"Creating thread with name: {thread_name}")

            # Create public thread by using start_thread instead of create_thread
            # This gives us more control over thread creation
            thread = await message.channel.create_thread(
                name=thread_name,
                auto_archive_duration=self.thread_auto_archive_duration,
                reason="Automatic announcement discussion thread",
                type=discord.ChannelType.public_thread,  # Explicitly set to public
                message=message  # Link to the announcement message
            )

            logger.info(f"Successfully created PUBLIC thread '{thread_name}' (ID: {thread.id}) for announcement")

            # Optional: Send an initial message in the thread
            await self.send_initial_thread_message(thread, message)

        except discord.Forbidden as e:
            logger.error(f"Missing permissions to create threads in {message.channel.name}: {e}")
        except discord.HTTPException as e:
            logger.error(f"Failed to create thread for announcement: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating thread: {e}", exc_info=True)

    def format_thread_name(self, message):
        """Format the thread name based on the config format and message content"""
        content = message.content

        # Remove role mentions <@&id> and channel mentions <#id>
        import re
        content = re.sub(r"<@&\d+>", "", content)   # roles
        content = re.sub(r"<#\d+>", "", content)    # channels
        # Optional: also remove user mentions <@id> / <@!id>
        content = re.sub(r"<@!?\d+>", "", content)

        # Normalize whitespace
        content = ' '.join(content.split())

        # Truncate to keep titles reasonable
        if len(content) > 50:
            content = content[:47] + "..."

        thread_name = self.thread_name_format.format(
            message_content=content,
            author_name=message.author.display_name,
            channel_name=message.channel.name
        )

        if len(thread_name) > 100:
            thread_name = thread_name[:97] + "..."

        return thread_name

    async def send_initial_thread_message(self, thread, original_message):
        """Send an initial welcome message in the newly created thread"""
        try:
            embed = discord.Embed(
                description=self.thread_welcome_message,
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Announcement by {original_message.author.display_name}")

            await thread.send(embed=embed)
            logger.info("Sent welcome message to thread")

        except Exception as e:
            logger.warning(f"Failed to send initial thread message: {e}")

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        """Handle thread cleanup when announcement message is deleted"""
        if not self.announcement_channel_id or payload.channel_id != self.announcement_channel_id:
            return

        if not self.auto_delete_threads:
            return

        try:
            # Get the channel and message data
            channel = self.bot.get_channel(payload.channel_id)
            if not channel:
                return

            # Try to get the thread that was associated with this message
            guild = channel.guild

            # Check if the deleted message had a thread
            thread = guild.get_thread(payload.message_id)
            if thread:
                await thread.delete(reason="Parent announcement message was deleted")
                logger.info(f"Deleted thread {thread.name} because parent message was deleted")
            else:
                logger.debug(f"No thread found for deleted message {payload.message_id}")

        except Exception as e:
            logger.error(f"Error handling thread cleanup for deleted message: {e}")


async def setup(bot):
    await bot.add_cog(AnnouncementThreadCog(bot))