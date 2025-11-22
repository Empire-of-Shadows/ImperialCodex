import discord
from discord.ext import commands, tasks
import logging

from configuration.config_system import config
from utils.logger import get_logger

# Logger
logger = get_logger("tag_tracker")

class TagTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tag_config = config.tag_tracker
        if self.tag_config.get("enabled"):
            self.check_tags.start()

    def cog_unload(self):
        self.check_tags.cancel()

    @tasks.loop(minutes=5)
    async def check_tags(self):
        logger.info("Starting tag check...")
        if not self.tag_config.get("enabled"):
            logger.info("Tag tracker is disabled in the configuration.")
            return

        server_tag = self.tag_config.get("server_tag")
        role_id = self.tag_config.get("role_id")

        if not server_tag or not role_id:
            logger.warning("Server tag or role ID is not configured. Aborting tag check.")
            return

        for guild in self.bot.guilds:
            role = guild.get_role(role_id)
            if not role:
                logger.warning(f"Role with ID {role_id} not found in guild {guild.name}. Skipping.")
                continue

            logger.info(f"Checking tags in guild: {guild.name}")
            async for member in guild.fetch_members(limit=None):
                if member.bot:
                    continue

                try:
                    user = await self.bot.fetch_user(member.id)
                    if user.primary_guild and user.primary_guild.tag == server_tag:
                        if role not in member.roles:
                            await member.add_roles(role)
                            logger.info(f"Added role {role.name} to {member.name} for having the tag.")
                    else:
                        if role in member.roles:
                            await member.remove_roles(role)
                            logger.info(f"Removed role {role.name} from {member.name} for not having the tag.")
                except discord.errors.NotFound:
                    logger.warning(f"Could not fetch user profile for member {member.name} ({member.id}). Skipping.")
                except Exception as e:
                    logger.error(f"An error occurred while checking tag for {member.name}: {e}")

        logger.info("Tag check finished.")

    @check_tags.before_loop
    async def before_check_tags(self):
        await self.bot.wait_until_ready()


async def setup(bot):
    await bot.add_cog(TagTracker(bot))
