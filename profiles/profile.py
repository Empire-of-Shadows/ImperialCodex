import os
import discord
from discord import app_commands
from discord.ext import commands
from utils.logger import get_logger
from dotenv import load_dotenv

load_dotenv()

path = os.getenv("PATH")
logger = get_logger("Profile")

class Profile(commands.Cog):
    """A cog for profile-related slash commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logger.info("Profile cog initialized")

    @app_commands.command(name="profile", description="Display your Ecom profile card.")
    @app_commands.describe(user="The user whose profile you want to see (optional)")
    async def profile(self, interaction: discord.Interaction, user: discord.User = None):
        """Slash command to display a user's profile card."""
        target_user = user or interaction.user
        logger.info(f"Profile command invoked for user={target_user.id}")

        profile_card_path = f"{path}{target_user.id}.png"

        # Defer the response as file operations might be slow
        await interaction.response.defer()

        if os.path.exists(profile_card_path):
            try:
                await interaction.followup.send(file=discord.File(profile_card_path))
                logger.info(f"Profile card sent for user={target_user.id}")
            except Exception as e:
                logger.error(f"Failed to send profile card for user {target_user.id}: {e}")
                await interaction.followup.send("Sorry, there was an error sending the profile card.", ephemeral=True)
        else:
            logger.warning(f"Profile card not found for user={target_user.id} at path: {profile_card_path}")
            await interaction.followup.send(
                "Your profile card isn't available yet. Please go to your profile page on the website and refresh your card.",
                ephemeral=True
            )


async def setup(bot: commands.Bot):
    """Function used to load the Profile cog."""
    logger.info("Setting up Profile cog")
    await bot.add_cog(Profile(bot))
    logger.info("Profile cog setup completed")