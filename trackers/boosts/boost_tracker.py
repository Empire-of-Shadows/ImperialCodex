import discord
from discord.ext import commands
import json
import datetime
import os
from typing import Dict, Any


class BoostTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.boost_data_file = 'boost_data.json'
        self.boost_data = self.load_boost_data()

    def load_boost_data(self) -> Dict[str, Any]:
        """Load boost data from JSON file"""
        try:
            with open(self.boost_data_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_boost_data(self):
        """Save boost data to JSON file"""
        with open(self.boost_data_file, 'w') as f:
            json.dump(self.boost_data, f, indent=4, default=str)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Track when members start or stop boosting"""

        # Member started boosting
        if not before.premium_since and after.premium_since:
            await self.log_boost_start(after)

        # Member stopped boosting
        elif before.premium_since and not after.premium_since:
            await self.log_boost_end(before)

    async def log_boost_start(self, member: discord.Member):
        """Log when a member starts boosting"""
        boost_info = {
            'user_id': str(member.id),
            'username': str(member),
            'boost_start': datetime.datetime.now().isoformat(),
            'guild_id': str(member.guild.id),
            'guild_name': str(member.guild.name),
            'current_boosts': member.guild.premium_subscription_count
        }

        # Store in memory and save to file
        self.boost_data[str(member.id)] = boost_info
        self.save_boost_data()

        # Send to log channel
        await self.send_boost_log(
            member.guild,
            f"🚀 **Server Boosted**\n"
            f"**User:** {member.mention} (`{member}`)\n"
            f"**Action:** Started boosting the server!\n"
            f"**Total Boosts:** {member.guild.premium_subscription_count}\n"
            f"**Boost Level:** {self.get_boost_level(member.guild.premium_subscription_count)}\n"
            f"**Time:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    async def log_boost_end(self, member: discord.Member):
        """Log when a member stops boosting"""
        user_data = self.boost_data.get(str(member.id), {})

        # Calculate boost duration if we have start time
        duration = "Unknown"
        if user_data and 'boost_start' in user_data:
            start_time = datetime.datetime.fromisoformat(user_data['boost_start'])
            end_time = datetime.datetime.now()
            duration = str(end_time - start_time).split('.')[0]  # Remove microseconds

        # Remove from active boosters
        if str(member.id) in self.boost_data:
            del self.boost_data[str(member.id)]
            self.save_boost_data()

        # Send to log channel
        await self.send_boost_log(
            member.guild,
            f"😔 **Boost Removed**\n"
            f"**User:** {member.mention} (`{member}`)\n"
            f"**Action:** Stopped boosting the server\n"
            f"**Boost Duration:** {duration}\n"
            f"**Total Boosts:** {member.guild.premium_subscription_count}\n"
            f"**Boost Level:** {self.get_boost_level(member.guild.premium_subscription_count)}\n"
            f"**Time:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    async def send_boost_log(self, guild: discord.Guild, message: str):
        """Send boost log to designated channel"""
        # Find boost-logs channel
        log_channel = discord.utils.get(guild.text_channels, name='boost-logs')

        if not log_channel:
            # Try to find any channel with 'log' in the name
            log_channel = discord.utils.get(guild.text_channels, name__contains='log')

        if log_channel:
            try:
                await log_channel.send(message)
            except discord.Forbidden:
                print(f"No permission to send messages in {log_channel.name}")

    def get_boost_level(self, boost_count: int) -> str:
        """Get the current boost level based on count"""
        if boost_count >= 14:
            return "Level 3 🚀"
        elif boost_count >= 7:
            return "Level 2 ⭐"
        elif boost_count >= 2:
            return "Level 1 ✨"
        else:
            return "No Level"

    @commands.command(name='boosters')
    async def list_boosters(self, ctx):
        """List all current server boosters"""
        current_boosters = []

        for member in ctx.guild.members:
            if member.premium_since:
                boost_info = self.boost_data.get(str(member.id), {})
                start_time = boost_info.get('boost_start', 'Unknown')

                if start_time != 'Unknown':
                    start_dt = datetime.datetime.fromisoformat(start_time)
                    duration = datetime.datetime.now() - start_dt
                    duration_str = str(duration).split('.')[0]
                else:
                    duration_str = 'Unknown'

                current_boosters.append({
                    'member': member,
                    'duration': duration_str,
                    'start_time': start_time
                })

        if not current_boosters:
            await ctx.send("No active boosters found.")
            return

        # Create embed with booster list
        embed = discord.Embed(
            title=f"🚀 Current Server Boosters - {len(current_boosters)}",
            color=0xff73fa
        )

        for booster in current_boosters:
            embed.add_field(
                name=f"{booster['member'].display_name}",
                value=f"Boosting for: {booster['duration']}",
                inline=False
            )

        await ctx.send(embed=embed)

    @commands.command(name='boosthistory')
    async def boost_history(self, ctx, user: discord.Member = None):
        """Check boost history for a user (or yourself)"""
        target_user = user or ctx.author
        user_data = self.boost_data.get(str(target_user.id))

        if user_data and 'boost_start' in user_data:
            start_time = datetime.datetime.fromisoformat(user_data['boost_start'])
            duration = datetime.datetime.now() - start_time

            embed = discord.Embed(
                title=f"Boost History - {target_user.display_name}",
                color=0x00ff00
            )
            embed.add_field(name="Started Boosting", value=start_time.strftime('%Y-%m-%d %H:%M:%S'), inline=False)
            embed.add_field(name="Duration", value=str(duration).split('.')[0], inline=False)
            embed.add_field(name="Total Server Boosts", value=ctx.guild.premium_subscription_count, inline=False)

            await ctx.send(embed=embed)
        else:
            await ctx.send(f"{target_user.mention} is not currently boosting or no data available.")


async def setup(bot):
    """Required setup function for cog loading"""
    await bot.add_cog(BoostTracker(bot))