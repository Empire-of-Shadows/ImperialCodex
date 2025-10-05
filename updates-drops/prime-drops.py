import asyncio
import os
from datetime import datetime, timezone, time
from typing import Optional, List, Dict, Any

import discord
from discord import app_commands
from discord.ext import commands, tasks
import pytz

from Database.DatabaseManager import db_manager
from utils.logger import get_logger

logger = get_logger("PrimeDrops")

# Configuration
DROPS_CHANNEL_ID = 1316889374318923856
CHICAGO_TZ = pytz.timezone('America/Chicago')
CHICAGO_TIME = time(5, 00)
UTC_TIME = datetime.combine(datetime.today(), CHICAGO_TIME)
UTC_TIME = CHICAGO_TZ.localize(UTC_TIME).astimezone(pytz.UTC).time()
SEND_TIME = UTC_TIME
ADMIN_ROLE_IDS = [1362166614451032346]



class DropsPaginator(discord.ui.View):
	"""Paginator for drops listings"""

	def __init__(self, embeds: List[discord.Embed], timeout: int = 300):
		super().__init__(timeout=timeout)
		self.embeds = embeds
		self.current_page = 0

		# Update button states
		self._update_buttons()

	def _update_buttons(self):
		"""Update button enabled/disabled states"""
		self.first_page.disabled = self.current_page == 0
		self.prev_page.disabled = self.current_page == 0
		self.next_page.disabled = self.current_page >= len(self.embeds) - 1
		self.last_page.disabled = self.current_page >= len(self.embeds) - 1

	@discord.ui.button(label='⏮️', style=discord.ButtonStyle.gray)
	async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
		self.current_page = 0
		self._update_buttons()
		await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

	@discord.ui.button(label='◀️', style=discord.ButtonStyle.gray)
	async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
		if self.current_page > 0:
			self.current_page -= 1
		self._update_buttons()
		await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

	@discord.ui.button(label='▶️', style=discord.ButtonStyle.gray)
	async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
		if self.current_page < len(self.embeds) - 1:
			self.current_page += 1
		self._update_buttons()
		await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

	@discord.ui.button(label='⏭️', style=discord.ButtonStyle.gray)
	async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
		self.current_page = len(self.embeds) - 1
		self._update_buttons()
		await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)


class PrimeDrops(commands.Cog):
	def __init__(self, bot: commands.Bot):
		self.bot = bot
		self.collection_manager = None

	async def cog_load(self):
		"""Initialize database when cog loads"""
		logger.info("Loading PrimeDrops cog...")
		await self.initialize_drops_database()

		# Start the daily task
		self.daily_drops_check.start()
		logger.info("PrimeDrops cog loaded successfully")

	async def cog_unload(self):
		"""Cleanup when cog unloads"""
		logger.info("Unloading PrimeDrops cog...")
		self.daily_drops_check.cancel()
		logger.info("PrimeDrops cog unloaded")

	async def initialize_drops_database(self):
		"""Initialize the drops database connection"""
		try:
			# Ensure database manager is initialized
			await db_manager.initialize()

			# Get the prime drops collection manager
			self.collection_manager = db_manager.get_collection_manager('prime_drops')

			logger.info("Prime drops database initialized successfully")
		except Exception as e:
			logger.error(f"Failed to initialize prime drops database: {e}", exc_info=True)
			raise

	def _has_admin_permissions(self, user: discord.Member) -> bool:
		"""Check if user has admin permissions"""
		if user.guild_permissions.administrator:
			return True

		user_role_ids = {role.id for role in user.roles}
		return any(role_id in user_role_ids for role_id in ADMIN_ROLE_IDS)

	def _create_drop_embed(self, drop: Dict[str, Any]) -> discord.Embed:
		"""Create an embed for a single drop"""
		embed = discord.Embed(
			title=drop.get('label', 'Unknown Game'),
			description=drop.get('description', 'No description available'),
			color=discord.Color.blue(),
			timestamp=datetime.now(timezone.utc)
		)

		expires = drop.get('expires')
		if expires:
			if isinstance(expires, str):
				embed.add_field(name="Expires", value=expires, inline=True)
			elif isinstance(expires, datetime):
				embed.add_field(name="Expires", value=expires.strftime("%Y-%m-%d %H:%M UTC"), inline=True)

		short_href = drop.get('short_href')
		if short_href:
			embed.add_field(name="Claim Here", value=f"[Get Free Game]({short_href})", inline=True)

		embed.set_footer(text="Amazon Prime Gaming Drops")

		return embed

	def _create_drops_embeds(self, drops: List[Dict[str, Any]], title: str) -> List[discord.Embed]:
		"""Create paginated embeds for multiple drops"""
		if not drops:
			embed = discord.Embed(
				title=title,
				description="No drops found.",
				color=discord.Color.orange()
			)
			return [embed]

		embeds = []
		drops_per_page = 5

		for i in range(0, len(drops), drops_per_page):
			page_drops = drops[i:i + drops_per_page]

			embed = discord.Embed(
				title=title,
				color=discord.Color.blue(),
				timestamp=datetime.now(timezone.utc)
			)

			for drop in page_drops:
				label = drop.get('label', 'Unknown Game')
				description = drop.get('description', 'No description')
				expires = drop.get('expires', 'Unknown')
				short_href = drop.get('short_href', '')
				sent = drop.get('sent', False)

				# Truncate description if too long
				if len(description) > 100:
					description = description[:97] + "..."

				status = "✅ Sent" if sent else "📤 Not Sent"
				link_text = f"[Link]({short_href})" if short_href else "No link"

				field_value = f"{description}\n**Expires:** {expires}\n**Status:** {status}\n**Link:** {link_text}"
				embed.add_field(name=label, value=field_value, inline=False)

			embed.set_footer(
				text=f"Page {len(embeds) + 1}/{(len(drops) + drops_per_page - 1) // drops_per_page} • {len(drops)} total drops"
			)
			embeds.append(embed)

		return embeds

	@tasks.loop(time=SEND_TIME)
	async def daily_drops_check(self):
		"""Daily task to check and send unsent drops"""
		try:
			logger.info("Running daily drops check...")

			if not self.collection_manager:
				logger.warning("Collection manager not initialized, skipping drops check")
				return

			unsent_drops = await self.collection_manager.find_many(
				{"sent": {"$ne": True}},
				sort=[("expires", 1)]
			)

			if not unsent_drops:
				logger.info("No unsent drops found")
				return

			logger.info(f"Found {len(unsent_drops)} unsent drops")

			channel = self.bot.get_channel(DROPS_CHANNEL_ID)
			if not channel:
				logger.error(f"Could not find drops channel with ID {DROPS_CHANNEL_ID}")
				return

			sent_count = 0
			for drop in unsent_drops:
				try:
					embed = self._create_drop_embed(drop)
					await channel.send(embed=embed)

					await self.collection_manager.update_one(
						{"_id": drop["_id"]},
						{"$set": {"sent": True, "sent_at": datetime.now(timezone.utc)}}
					)

					sent_count += 1
					logger.info(f"Sent drop: {drop.get('label', 'Unknown')}")

					# Rate limiting - wait 1 second between sends
					if sent_count < len(unsent_drops):
						await asyncio.sleep(1)

				except Exception as e:
					logger.error(f"Failed to send drop {drop.get('label', 'Unknown')}: {e}")
					continue

			logger.info(f"Daily drops check completed. Sent {sent_count} drops.")

		except Exception as e:
			logger.error(f"Error in daily drops check: {e}", exc_info=True)

	@daily_drops_check.before_loop
	async def before_daily_drops_check(self):
		"""Wait for bot to be ready before starting daily task"""
		await self.bot.wait_until_ready()
		logger.info(f"Daily drops check scheduled for 6:00 AM Chicago time (UTC: {SEND_TIME})")

	# Slash Commands
	@app_commands.command(name="drops", description="Browse all Prime Gaming drops")
	async def drops_command(self, interaction: discord.Interaction):
		"""Show all drops with pagination"""
		try:
			await interaction.response.defer(thinking=True)

			if not self.collection_manager:
				await interaction.followup.send("Database not initialized. Please try again later.", ephemeral=True)
				return

			# Get all drops, sorted by expiration date
			drops = await self.collection_manager.find_many(
				{},
				sort=[("expires", 1)]
			)

			embeds = self._create_drops_embeds(drops, "🎮 All Prime Gaming Drops")

			if len(embeds) == 1:
				await interaction.followup.send(embed=embeds[0])
			else:
				view = DropsPaginator(embeds)
				await interaction.followup.send(embed=embeds[0], view=view)

		except Exception as e:
			logger.error(f"Error in drops command: {e}", exc_info=True)
			await interaction.followup.send("An error occurred while fetching drops.", ephemeral=True)

	@app_commands.command(name="drops-unsent", description="[Admin] Show unsent drops")
	async def drops_unsent_command(self, interaction: discord.Interaction):
		"""Admin command to show unsent drops"""
		if not self._has_admin_permissions(interaction.user):
			await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
			return

		try:
			await interaction.response.defer(thinking=True)

			if not self.collection_manager:
				await interaction.followup.send("Database not initialized. Please try again later.", ephemeral=True)
				return

			# Get unsent drops
			drops = await self.collection_manager.find_many(
				{"sent": {"$ne": True}},
				sort=[("expires", 1)]
			)

			embeds = self._create_drops_embeds(drops, "📤 Unsent Prime Gaming Drops")

			if len(embeds) == 1:
				await interaction.followup.send(embed=embeds[0], ephemeral=True)
			else:
				view = DropsPaginator(embeds)
				await interaction.followup.send(embed=embeds[0], view=view, ephemeral=True)

		except Exception as e:
			logger.error(f"Error in drops-unsent command: {e}", exc_info=True)
			await interaction.followup.send("An error occurred while fetching unsent drops.", ephemeral=True)

	@app_commands.command(name="drops-sent", description="[Admin] Show sent drops")
	async def drops_sent_command(self, interaction: discord.Interaction):
		"""Admin command to show sent drops"""
		if not self._has_admin_permissions(interaction.user):
			await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
			return

		try:
			await interaction.response.defer(thinking=True)

			if not self.collection_manager:
				await interaction.followup.send("Database not initialized. Please try again later.", ephemeral=True)
				return

			# Get sent drops
			drops = await self.collection_manager.find_many(
				{"sent": True},
				sort=[("expires", 1)]
			)

			embeds = self._create_drops_embeds(drops, "✅ Sent Prime Gaming Drops")

			if len(embeds) == 1:
				await interaction.followup.send(embed=embeds[0], ephemeral=True)
			else:
				view = DropsPaginator(embeds)
				await interaction.followup.send(embed=embeds[0], view=view, ephemeral=True)

		except Exception as e:
			logger.error(f"Error in drops-sent command: {e}", exc_info=True)
			await interaction.followup.send("An error occurred while fetching sent drops.", ephemeral=True)

	@app_commands.command(name="drops-test", description="[Admin] Test the daily drops check")
	async def drops_test_command(self, interaction: discord.Interaction):
		"""Admin command to manually trigger drops check"""
		if not self._has_admin_permissions(interaction.user):
			await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
			return

		try:
			await interaction.response.defer(thinking=True)

			# Run the daily check manually
			await self.daily_drops_check()

			await interaction.followup.send("Manual drops check completed. Check the drops channel for results.",
											ephemeral=True)

		except Exception as e:
			logger.error(f"Error in drops-test command: {e}", exc_info=True)
			await interaction.followup.send("An error occurred while running the drops check.", ephemeral=True)


async def setup(bot: commands.Bot):
	await bot.add_cog(PrimeDrops(bot))