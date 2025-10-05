import random
import logging
import os
from datetime import datetime, timedelta, timezone
import pytz
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

from utils.bot import s
from utils.logger import get_logger, PerformanceLogger
from Database.DatabaseManager import db_manager

# Load environment variables
load_dotenv()

# Constants
POST_CHANNEL_ID = 1424454705433542758  # Channel where questions are posted
OPTION1_EMOJI = "1️⃣"  # Reaction for option 1
OPTION2_EMOJI = "2️⃣"  # Reaction for option 2

# Scheduling constants
TARGET_HOUR = 6
TARGET_TIMEZONE = pytz.timezone("America/Chicago")

logger = get_logger("WYR")


class WYRCommandGroup(app_commands.Group):
    """Command group for Would You Rather commands"""

    def __init__(self, cog):
        super().__init__(name="wyr", description="Would You Rather commands")
        self.cog = cog

    @app_commands.command(name="post", description="Manually post a WYR question (Admin only)")
    @app_commands.describe(
        category="Category of question (sfw, nsfw, mixed)",
        random_pick="Pick a random question instead of least used"
    )
    @app_commands.default_permissions(manage_messages=True)
    async def post_wyr(self, interaction: discord.Interaction, category: str = "sfw", random_pick: bool = False):
        """
        Manually post a WYR question.
        """
        logger.info(
            f"Manual WYR post requested by {interaction.user} (ID: {interaction.user.id}) - Category: {category}, Random: {random_pick}")

        try:
            with PerformanceLogger(logger, f"post_wyr_command_{category}"):
                if random_pick:
                    question = await self.cog.get_random_question(category)
                else:
                    question = await self.cog.get_next_question(category)

                if not question:
                    logger.warning(f"No {category} questions available for manual post by {interaction.user}")
                    await interaction.response.send_message(f"There are no {category} questions available right now.",
                                                            ephemeral=True)
                    return

                embed = self.cog.create_question_embed(question)
                view = WYRView(question["_id"], self.cog)

                await interaction.response.send_message(embed=embed, view=view)
                message = await interaction.original_response()

                # Store the message-question mapping
                await self.cog.store_message_question_mapping(message.id, question["_id"])

                # Create a discussion thread
                thread = await message.create_thread(
                    name=f" WYR Discussion - {datetime.now().strftime('%m/%d')}",
                    auto_archive_duration=1440
                )

                await thread.send(" **What's your reasoning?** Share your thoughts on this choice!")
                # await self.cog.increment_used_count(question["_id"])

                logger.info(f"Successfully posted manual WYR question {question['_id']} in thread {thread.id}")

        except Exception as e:
            logger.error(f"Error in manual WYR post by {interaction.user}: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ An error occurred while posting the question.",
                                                        ephemeral=True)

    @app_commands.command(name="stats", description="Check WYR voting statistics for yourself or another user")
    @app_commands.describe(user="User to check stats for (defaults to yourself)")
    async def wyr_stats(self, interaction: discord.Interaction, user: discord.Member = None):
        """
        Check WYR voting statistics for yourself or another user.
        """
        target_user = user or interaction.user
        logger.info(f"WYR stats requested by {interaction.user} for user {target_user} (ID: {target_user.id})")

        try:
            with PerformanceLogger(logger, f"wyr_stats_lookup_{target_user.id}"):
                stats = await self.cog.get_user_stats(target_user.id)

                embed = discord.Embed(
                    title=f" WYR Stats for {target_user.display_name}",
                    color=discord.Color.green()
                )

                embed.add_field(
                    name=f"{OPTION1_EMOJI} Option 1 Votes",
                    value=f"{stats['option1_votes']:,}",
                    inline=True
                )
                embed.add_field(
                    name=f"{OPTION2_EMOJI} Option 2 Votes",
                    value=f"{stats['option2_votes']:,}",
                    inline=True
                )
                embed.add_field(
                    name="️ Total Votes",
                    value=f"{stats['total_votes']:,}",
                    inline=True
                )

                if stats['total_votes'] > 0:
                    option1_pct = (stats['option1_votes'] / stats['total_votes']) * 100
                    option2_pct = (stats['option2_votes'] / stats['total_votes']) * 100
                    embed.add_field(
                        name=" Voting Preference",
                        value=f"Option 1: {option1_pct:.1f}%\nOption 2: {option2_pct:.1f}%",
                        inline=False
                    )

                # Add timestamps if available
                if stats.get('first_vote'):
                    embed.add_field(
                        name=" First Vote",
                        value=f"<t:{int(stats['first_vote'].timestamp())}:R>",
                        inline=True
                    )
                if stats.get('last_vote'):
                    embed.add_field(
                        name=" Last Vote",
                        value=f"<t:{int(stats['last_vote'].timestamp())}:R>",
                        inline=True
                    )

                embed.set_thumbnail(url=target_user.display_avatar.url)
                await interaction.response.send_message(embed=embed)

                logger.info(f"WYR stats successfully displayed for {target_user} (Total votes: {stats['total_votes']})")

        except Exception as e:
            logger.error(f"Error retrieving WYR stats for {target_user}: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while fetching stats.", ephemeral=True)

    @app_commands.command(name="results", description="Show results for a specific WYR question")
    @app_commands.describe(message_id="Message ID of the WYR question to check results for")
    async def wyr_results(self, interaction: discord.Interaction, message_id: str = None):
        """
        Show results for a specific WYR question.
        """
        logger.info(f"WYR results requested by {interaction.user} for message ID: {message_id}")

        if not message_id:
            logger.warning(f"WYR results request missing message ID from {interaction.user}")
            await interaction.response.send_message(
                "Please provide the message ID of the WYR question you want to check results for.", ephemeral=True)
            return

        try:
            # Get question ID from mapping
            question_id = await self.cog.get_question_id_from_message(int(message_id))
            if not question_id:
                logger.warning(f"No question mapping found for message ID {message_id} by {interaction.user}")
                await interaction.response.send_message(
                    "No WYR question found for that message ID. It might be from an older post.", ephemeral=True)
                return

            # Get results using the question ID
            results = await self.cog.get_question_results(question_id)
            if not results:
                logger.warning(f"Could not fetch results for question {question_id} from message {message_id}")
                await interaction.response.send_message("❌ Could not fetch results for that question.", ephemeral=True)
                return

            # Create results embed
            embed = discord.Embed(
                title="📊 WYR Results",
                color=discord.Color.green()
            )

            # Create visual progress bars
            def create_bar(percentage, length=20):
                filled = int(percentage / 100 * length)
                return "█" * filled + "░" * (length - filled)

            bar1 = create_bar(results['option1_percentage'])
            bar2 = create_bar(results['option2_percentage'])

            embed.add_field(
                name=f"{OPTION1_EMOJI} Option 1",
                value=f"{bar1} {results['option1_percentage']:.1f}% ({results['option1_votes']} votes)",
                inline=False
            )
            embed.add_field(
                name=f"{OPTION2_EMOJI} Option 2",
                value=f"{bar2} {results['option2_percentage']:.1f}% ({results['option2_votes']} votes)",
                inline=False
            )
            embed.add_field(
                name=" Total Votes",
                value=f"{results['total_votes']} people have voted",
                inline=False
            )

            await interaction.response.send_message(embed=embed)
            logger.info(f"Successfully showed results for question {question_id} via command")

        except (ValueError, discord.NotFound):
            logger.warning(f"Invalid or not found message ID {message_id} requested by {interaction.user}")
            await interaction.response.send_message("Invalid message ID or message not found.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error fetching WYR results for message {message_id}: {e}", exc_info=True)
            await interaction.response.send_message(f"Error fetching results: {e}", ephemeral=True)

    @app_commands.command(name="leaderboard", description="Show the WYR voting leaderboard")
    @app_commands.describe(limit="Number of users to show in leaderboard (default: 10)")
    async def wyr_leaderboard(self, interaction: discord.Interaction, limit: int = 10):
        """
        Show the WYR voting leaderboard using the dedicated leaderboard collection.
        """
        logger.info(f"WYR leaderboard requested by {interaction.user} with limit: {limit}")

        try:
            with PerformanceLogger(logger, f"wyr_leaderboard_generation_limit_{limit}"):
                # Get top users from leaderboard collection using the new database manager
                top_users = await db_manager.daily_wyr_leaderboard.find_many(
                    sort=[("total_votes", -1)],
                    limit=limit
                )

                if not top_users:
                    logger.info("No WYR voting data available for leaderboard")
                    await interaction.response.send_message("No voting data available yet!")
                    return

                embed = discord.Embed(
                    title=" WYR Voting Leaderboard",
                    description="Most active voters in Would You Rather questions",
                    color=discord.Color.gold()
                )

                leaderboard_text = ""
                for i, user_data in enumerate(top_users, 1):
                    try:
                        user = await self.cog.bot.fetch_user(int(user_data["user_id"]))
                        emoji = "" if i == 1 else "" if i == 2 else "" if i == 3 else ""
                        vote_count = user_data["total_votes"]
                        leaderboard_text += f"{emoji} **{i}.** {user.mention} - {vote_count:,} votes\n"
                    except:
                        vote_count = user_data["total_votes"]
                        leaderboard_text += f" **{i}.** Unknown User - {vote_count:,} votes\n"
                        logger.warning(f"Could not fetch user data for user ID {user_data.get('user_id')}")

                embed.description = leaderboard_text
                embed.set_footer(text=f"Showing top {min(limit, len(top_users))} voters")

                await interaction.response.send_message(embed=embed)
                logger.info(f"WYR leaderboard successfully generated with {len(top_users)} users")

        except Exception as e:
            logger.error(f"Error generating WYR leaderboard: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while generating the leaderboard.",
                                                    ephemeral=True)

    @app_commands.command(name="reset_stats", description="Reset a user's WYR statistics (Admin only)")
    @app_commands.describe(user="User to reset stats for")
    @app_commands.default_permissions(administrator=True)
    async def wyr_reset_stats(self, interaction: discord.Interaction, user: discord.Member):
        """
        Reset a user's WYR statistics (Admin only).
        """
        logger.warning(f"WYR stats reset requested by {interaction.user} for {user} (ID: {user.id})")

        try:
            with PerformanceLogger(logger, f"wyr_stats_reset_{user.id}"):
                # Use the new database manager to delete user stats
                success = await db_manager.daily_wyr_leaderboard.delete_one({"user_id": str(user.id)})

                if success:
                    embed = discord.Embed(
                        title="✅ Stats Reset",
                        description=f"Successfully reset WYR statistics for {user.mention}",
                        color=discord.Color.green()
                    )
                    logger.info(f"Successfully reset WYR stats for {user} (ID: {user.id})")
                else:
                    embed = discord.Embed(
                        title="ℹ️ No Stats Found",
                        description=f"No WYR statistics found for {user.mention}",
                        color=discord.Color.blue()
                    )
                    logger.info(f"No WYR stats found to reset for {user} (ID: {user.id})")

                await interaction.response.send_message(embed=embed)

        except Exception as e:
            logger.error(f"Error resetting WYR stats for {user}: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while resetting stats.", ephemeral=True)


class WYR(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.loop.create_task(self.initialize_database())
        self.bot.loop.create_task(self._register_views())
        # Add the command group to the bot
        self.wyr_commands = WYRCommandGroup(self)
        self.bot.tree.add_command(self.wyr_commands)

        logger.info("WYR cog initialized - starting database initialization")

    async def cog_unload(self):
        """Clean up when cog is unloaded"""
        logger.info("WYR cog unloading - cleaning up")

        self.bot.tree.remove_command("wyr")
        logger.info("WYR command group removed from bot tree")

    async def _register_views(self):
        """Register persistent views after bot is ready"""
        await self.bot.wait_until_ready()
        # Register the view without question_id and cog - they will be set when needed
        self.bot.add_view(WYRView())
        logger.info("Persistent WYRView registered after bot ready")

    async def initialize_database(self):
        """
        Initialize the database connection using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, "wyr_database_initialization"):
                # Initialize the global database manager if not already initialized
                if not db_manager._initialized:
                    await db_manager.initialize()

                logger.info(f"{s}✅ WYR database initialized successfully")

                # Start the timer after database is ready
                await self.schedule_next_post()

        except Exception as e:
            logger.error(f"{s}❌ Failed to initialize WYR database: {e}", exc_info=True)

    async def store_message_question_mapping(self, message_id, question_id):
        """
        Store the relationship between message ID and question ID in the database.
        """
        try:
            with PerformanceLogger(logger, f"store_mapping_{message_id}"):
                mapping_data = {
                    "message_id": str(message_id),
                    "question_id": question_id,
                    "created_at": datetime.now(timezone.utc),
                    "channel_id": POST_CHANNEL_ID  # Store channel ID for reference
                }

                # Use the new database manager to create the mapping
                await db_manager.daily_wyr_mappings.create_one(mapping_data)
                logger.info(f"Stored message-question mapping: message {message_id} -> question {question_id}")

        except Exception as e:
            logger.error(f"Error storing message-question mapping for message {message_id}: {e}", exc_info=True)

    async def get_question_id_from_message(self, message_id):
        """
        Get question ID from message ID using the stored mapping.
        """
        try:
            with PerformanceLogger(logger, f"get_question_id_{message_id}"):
                # Use the new database manager to find the mapping
                mapping = await db_manager.daily_wyr_mappings.find_one({"message_id": str(message_id)})

                if mapping:
                    question_id = mapping.get("question_id")
                    logger.info(f"Retrieved question ID {question_id} for message {message_id}")
                    return question_id
                else:
                    logger.warning(f"No mapping found for message ID {message_id}")
                    return None

        except Exception as e:
            logger.error(f"Error retrieving question ID for message {message_id}: {e}", exc_info=True)
            return None

    async def get_message_id_from_question(self, question_id):
        """
        Get message ID from question ID using the stored mapping.
        """
        try:
            with PerformanceLogger(logger, f"get_message_id_{question_id}"):
                # Use the new database manager to find the mapping
                mapping = await db_manager.daily_wyr_mappings.find_one({"question_id": question_id})

                if mapping:
                    message_id = mapping.get("message_id")
                    logger.info(f"Retrieved message ID {message_id} for question {question_id}")
                    return int(message_id) if message_id else None
                else:
                    logger.warning(f"No mapping found for question ID {question_id}")
                    return None

        except Exception as e:
            logger.error(f"Error retrieving message ID for question {question_id}: {e}", exc_info=True)
            return None

    async def cleanup_old_mappings(self, days_old=30):
        """
        Clean up old message-question mappings to prevent database bloat.
        """
        try:
            with PerformanceLogger(logger, "cleanup_old_mappings"):
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

                # Delete mappings older than the cutoff date
                result = await db_manager.daily_wyr_mappings.delete_many({
                    "created_at": {"$lt": cutoff_date}
                })

                logger.info(f"Cleaned up {result} old message-question mappings older than {days_old} days")

        except Exception as e:
            logger.error(f"Error cleaning up old mappings: {e}", exc_info=True)

    async def get_next_6am_chicago(self):
        """
        Calculate the next 6 AM Chicago time from now.
        """
        try:
            # Get current time in Chicago timezone
            chicago_now = datetime.now(TARGET_TIMEZONE)
            logger.info(f"Current Chicago time: {chicago_now}")

            # Create today's 6 AM in Chicago timezone
            today_6am = chicago_now.replace(hour=TARGET_HOUR, minute=0, second=0, microsecond=0)

            # If it's already past 6 AM today, schedule for tomorrow
            if chicago_now >= today_6am:
                next_6am = today_6am + timedelta(days=1)
            else:
                next_6am = today_6am

            logger.info(f"Next scheduled WYR post: {next_6am} ({next_6am.strftime('%A, %B %d at %I:%M %p %Z')})")
            return next_6am

        except Exception as e:
            logger.error(f"Error calculating next 6 AM Chicago time: {e}", exc_info=True)
            # Fallback: schedule for 1 hour from now
            return datetime.now(TARGET_TIMEZONE) + timedelta(hours=1)

    async def schedule_next_post(self):
        """
        Schedule the next WYR post for 6 AM Chicago time.
        """
        try:
            with PerformanceLogger(logger, "schedule_next_wyr_post"):
                next_post_time = await self.get_next_6am_chicago()

                # Convert to UTC for discord.utils.sleep_until
                next_post_utc = next_post_time.astimezone(timezone.utc)

                # Calculate time until next post
                now_utc = datetime.now(timezone.utc)
                time_until_post = next_post_utc - now_utc

                logger.info(f"Scheduling next WYR post in {time_until_post} at {next_post_utc} UTC")

                # Sleep until the scheduled time
                await discord.utils.sleep_until(next_post_utc)

                # Post the question
                await self.post_daily_question()

                # Schedule the next post (24 hours later)
                self.bot.loop.create_task(self.schedule_next_post())

        except Exception as e:
            logger.error(f"Error scheduling next WYR post: {e}", exc_info=True)
            # Fallback: try again in 1 hour
            await discord.utils.sleep_until(datetime.now(timezone.utc) + timedelta(hours=1))
            self.bot.loop.create_task(self.schedule_next_post())

    async def post_daily_question(self):
        """
        Post a daily WYR question in the designated channel.
        """
        logger.info("Posting scheduled daily WYR question (6 AM Chicago time)")

        try:
            with PerformanceLogger(logger, "scheduled_daily_wyr_post"):
                question = await self.get_next_question()
                if not question:
                    logger.warning("No SFW questions available for scheduled daily post - skipping")
                    return

                channel = self.bot.get_channel(POST_CHANNEL_ID)
                if not channel:
                    logger.error(f"Channel with ID {POST_CHANNEL_ID} not found for scheduled daily post")
                    return

                embed = self.create_question_embed(question)
                view = WYRView(question["_id"], self)
                message = await channel.send(content="<@&1392926433734820014>", embed=embed, view=view)

                # Store the message-question mapping
                await self.store_message_question_mapping(message.id, question["_id"])

                # Create a discussion thread
                chicago_now = datetime.now(TARGET_TIMEZONE)
                thread = await message.create_thread(
                    name=f" WYR Discussion - {chicago_now.strftime('%m/%d')}",
                    auto_archive_duration=1440
                )

                # Send a starter message in the thread
                await thread.send(" **What's your reasoning?** Share your thoughts on this choice!")

                await self.increment_used_count(question["_id"])

                logger.info(
                    f"Successfully posted scheduled daily WYR question {question['_id']} in channel {POST_CHANNEL_ID} with thread {thread.id}")

        except Exception as e:
            logger.error(f"Error in scheduled daily WYR post: {e}", exc_info=True)

    async def update_user_leaderboard(self, user_id, option_chosen):
        """
        Update user statistics in the WYR_Leaderboard collection using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, f"update_user_leaderboard_{user_id}"):
                user_id_str = str(user_id)

                # Check if user exists in leaderboard
                user_stats = await db_manager.daily_wyr_leaderboard.find_one({"user_id": user_id_str})

                if not user_stats:
                    # Create new user entry
                    new_user = {
                        "user_id": user_id_str,
                        "total_votes": 1,
                        "option1_votes": 1 if option_chosen == "option1" else 0,
                        "option2_votes": 1 if option_chosen == "option2" else 0,
                        "last_vote": datetime.now(timezone.utc),
                        "first_vote": datetime.now(timezone.utc)
                    }
                    await db_manager.daily_wyr_leaderboard.create_one(new_user)
                    logger.info(f"Created new leaderboard entry for user {user_id}: {option_chosen}")
                else:
                    # Update existing user
                    update_query = {
                        "$inc": {
                            "total_votes": 1,
                            f"{option_chosen}_votes": 1
                        },
                        "$set": {
                            "last_vote": datetime.now(timezone.utc)
                        }
                    }
                    await db_manager.daily_wyr_leaderboard.update_one(
                        {"user_id": user_id_str},
                        update_query
                    )
                    logger.info(
                        f"Updated leaderboard for user {user_id}: {option_chosen} (total: {user_stats.get('total_votes', 0) + 1})")

        except Exception as e:
            logger.error(f"Error updating user leaderboard for {user_id}: {e}", exc_info=True)

    async def get_next_question(self, category="sfw", exclude_used=False):
        """
        Fetch the next "Would You Rather" question with specified criteria using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, f"get_next_question_{category}"):
                query = {"tags": category}
                if exclude_used:
                    query["used_count"] = {"$eq": 0}

                # Use the new database manager to find questions
                questions = await db_manager.daily_wyr.find_many(
                    filter_dict=query,
                    sort=[("used_count", 1)],
                    limit=1
                )

                if questions:
                    question = questions[0]
                    logger.info(
                        f"Retrieved next {category} question: ID {question['_id']} (used_count: {question.get('used_count', 0)})")
                    return question
                else:
                    logger.warning(f"No {category} questions available (exclude_used: {exclude_used})")
                    return None

        except Exception as e:
            logger.error(f"Error fetching next WYR question ({category}): {e}", exc_info=True)
            return None

    async def get_random_question(self, category="sfw"):
        """
        Get a random question from the specified category using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, f"get_random_question_{category}"):
                pipeline = [
                    {"$match": {"tags": category}},
                    {"$sample": {"size": 1}}
                ]

                # Use the new database manager for aggregation
                questions = await db_manager.daily_wyr.aggregate(pipeline)

                if questions:
                    question = questions[0]
                    logger.info(f"Retrieved random {category} question: ID {question['_id']}")
                    return question
                else:
                    logger.warning(f"No {category} questions available for random selection")
                    return None

        except Exception as e:
            logger.error(f"Error fetching random WYR question ({category}): {e}", exc_info=True)
            return None

    async def get_user_stats(self, user_id):
        """
        Get user voting statistics from the leaderboard collection using the new DatabaseManager.
        """
        default_stats = {"option1_votes": 0, "option2_votes": 0, "total_votes": 0}

        try:
            with PerformanceLogger(logger, f"get_user_stats_{user_id}"):
                # Use the new database manager to find user stats
                user_stats = await db_manager.daily_wyr_leaderboard.find_one({"user_id": str(user_id)})

                if not user_stats:
                    logger.info(f"No stats found for user {user_id}")
                    return default_stats

                stats = {
                    "option1_votes": user_stats.get("option1_votes", 0),
                    "option2_votes": user_stats.get("option2_votes", 0),
                    "total_votes": user_stats.get("total_votes", 0),
                    "first_vote": user_stats.get("first_vote"),
                    "last_vote": user_stats.get("last_vote")
                }

                logger.info(f"Retrieved stats for user {user_id}: {stats['total_votes']} total votes")
                return stats

        except Exception as e:
            logger.error(f"Error fetching user stats for {user_id}: {e}", exc_info=True)
            return default_stats

    async def record_vote(self, question_id, user_id, option):
        """
        Record a user's vote for a question and update leaderboard using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, f"record_vote_{user_id}_{option}"):
                # Check if user has already voted to handle vote count properly
                existing_question = await db_manager.daily_wyr.find_one({"_id": question_id})
                if not existing_question:
                    logger.error(f"Question {question_id} not found for vote recording")
                    return

                existing_votes = existing_question.get("votes", {})
                previous_vote = existing_votes.get(str(user_id))

                update_query = {"$set": {f"votes.{user_id}": option}}
                is_new_vote = not previous_vote

                # If this is a new vote, increment the chosen option
                if is_new_vote:
                    update_query["$inc"] = {f"vote_counts.{option}": 1}
                # If changing vote, decrement old option and increment new option
                elif previous_vote != option:
                    update_query["$inc"] = {
                        f"vote_counts.{previous_vote}": -1,
                        f"vote_counts.{option}": 1
                    }

                # Use the new database manager to update the question
                await db_manager.daily_wyr.update_one({"_id": question_id}, update_query)

                # Only update leaderboard for new votes (not vote changes)
                if is_new_vote:
                    await self.update_user_leaderboard(user_id, option)

                vote_type = "new" if is_new_vote else "changed" if previous_vote != option else "duplicate"
                logger.info(f"Recorded {vote_type} vote for user {user_id} on question {question_id}: {option}")

        except Exception as e:
            logger.error(f"Error recording vote (user: {user_id}, question: {question_id}, option: {option}): {e}",
                         exc_info=True)

    async def get_question_results(self, question_id):
        """
        Get voting results for a specific question using the new DatabaseManager.
        """
        try:
            with PerformanceLogger(logger, f"get_question_results_{question_id}"):
                # Use the new database manager to find the question
                question = await db_manager.daily_wyr.find_one({"_id": question_id})
                if not question:
                    logger.warning(f"Question {question_id} not found for results")
                    return None

                vote_counts = question.get("vote_counts", {"option1": 0, "option2": 0})
                total_votes = vote_counts.get("option1", 0) + vote_counts.get("option2", 0)

                if total_votes > 0:
                    option1_percentage = (vote_counts.get("option1", 0) / total_votes) * 100
                    option2_percentage = (vote_counts.get("option2", 0) / total_votes) * 100
                else:
                    option1_percentage = option2_percentage = 0

                results = {
                    "option1_votes": vote_counts.get("option1", 0),
                    "option2_votes": vote_counts.get("option2", 0),
                    "option1_percentage": option1_percentage,
                    "option2_percentage": option2_percentage,
                    "total_votes": total_votes
                }

                logger.info(f"Retrieved results for question {question_id}: {total_votes} total votes")
                return results

        except Exception as e:
            logger.error(f"Error getting question results for {question_id}: {e}", exc_info=True)
            return None

    async def increment_used_count(self, question_id):
        """
        Increment the `used_count` for a specific question using the new DatabaseManager.
        """
        try:
            # Use the new database manager to update the used count
            success = await db_manager.daily_wyr.update_one(
                {"_id": question_id},
                {"$inc": {"used_count": 1}}
            )

            if success:
                logger.info(f"Incremented used_count for question {question_id}")
            else:
                logger.warning(f"No document modified when incrementing used_count for question {question_id}")

        except Exception as e:
            logger.error(f"Error updating used_count for question {question_id}: {e}", exc_info=True)

    def create_question_embed(self, question, show_results=False, results=None):
        """
        Create a Discord embed for the WYR question.
        """
        try:
            embed = discord.Embed(
                title="❓ Would You Rather...",
                description=(
                    f"{OPTION1_EMOJI} **{question['option1']}**\n"
                    f"{OPTION2_EMOJI} **{question['option2']}**"
                ),
                color=discord.Color.blue()
            )

            if show_results and results:
                embed.add_field(
                    name=" Current Results",
                    value=(
                        f"{OPTION1_EMOJI} **{results['option1_percentage']:.1f}%** "
                        f"({results['option1_votes']} votes)\n"
                        f"{OPTION2_EMOJI} **{results['option2_percentage']:.1f}%** "
                        f"({results['option2_votes']} votes)\n\n"
                        f"**Total Votes:** {results['total_votes']}"
                    ),
                    inline=False
                )

            embed.set_footer(text="Click a button to vote! • Results update in real-time")
            logger.debug(f"Created embed for question {question.get('_id', 'unknown')}")
            return embed

        except Exception as e:
            logger.error(f"Error creating question embed: {e}", exc_info=True)
            # Return a basic error embed
            return discord.Embed(
                title="❌ Error",
                description="Failed to create question embed",
                color=discord.Color.red()
            )


class WYRView(discord.ui.View):
    def __init__(self, question_id=None, cog=None):
        super().__init__(timeout=None)
        self.question_id = question_id
        self.cog = cog
        if question_id:
            logger.debug(f"Created WYRView for question {question_id}")

    def _get_cog(self, interaction: discord.Interaction):
        """Get the cog instance from the bot"""
        cog = interaction.client.get_cog("WYR")
        if not cog:
            logger.error("WYR cog not found when handling button interaction")
            raise RuntimeError("WYR cog not available")
        return cog

    @discord.ui.button(label="Option 1", style=discord.ButtonStyle.primary, emoji=OPTION1_EMOJI,
                       custom_id="wyr:option1")
    async def option1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(
            f"Option 1 vote button clicked by {interaction.user} (ID: {interaction.user.id}) for question {self.question_id}")
        await self.handle_vote(interaction, "option1")

    @discord.ui.button(label="Option 2", style=discord.ButtonStyle.primary, emoji=OPTION2_EMOJI,
                       custom_id="wyr:option2")
    async def option2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(
            f"Option 2 vote button clicked by {interaction.user} (ID: {interaction.user.id}) for question {self.question_id}")
        await self.handle_vote(interaction, "option2")

    @discord.ui.button(label="Show Results", style=discord.ButtonStyle.secondary, custom_id="wyr:results")
    async def show_results_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(
            f"Show results button clicked by {interaction.user} (ID: {interaction.user.id}) for question {self.question_id}")

        try:
            with PerformanceLogger(logger, f"show_results_{self.question_id}"):
                # Get the cog instance dynamically
                cog = self._get_cog(interaction)

                # Extract question_id from the message using mapping
                question_id = self.question_id
                if not question_id:
                    question_id = await cog.get_question_id_from_message(interaction.message.id)

                if not question_id:
                    logger.error(f"Could not determine question ID for results request from {interaction.user}")
                    await interaction.response.send_message("❌ Could not determine which question to show results for.",
                                                            ephemeral=True)
                    return

                results = await cog.get_question_results(question_id)
                if not results:
                    logger.warning(f"Could not fetch results for question {question_id}")
                    await interaction.response.send_message("❌ Could not fetch results.", ephemeral=True)
                    return

                embed = discord.Embed(
                    title=" Current Results",
                    color=discord.Color.green()
                )

                # Create visual progress bars
                def create_bar(percentage, length=20):
                    filled = int(percentage / 100 * length)
                    return "█" * filled + "░" * (length - filled)

                bar1 = create_bar(results['option1_percentage'])
                bar2 = create_bar(results['option2_percentage'])

                embed.add_field(
                    name=f"{OPTION1_EMOJI} Option 1",
                    value=f"{bar1} {results['option1_percentage']:.1f}% ({results['option1_votes']} votes)",
                    inline=False
                )
                embed.add_field(
                    name=f"{OPTION2_EMOJI} Option 2",
                    value=f"{bar2} {results['option2_percentage']:.1f}% ({results['option2_votes']} votes)",
                    inline=False
                )
                embed.add_field(
                    name=" Total Votes",
                    value=f"{results['total_votes']} people have voted",
                    inline=False
                )

                await interaction.response.send_message(embed=embed, ephemeral=True)
                logger.info(f"Successfully showed results for question {question_id} to {interaction.user}")

        except Exception as e:
            logger.error(f"Error showing results: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while fetching results.", ephemeral=True)

    async def handle_vote(self, interaction: discord.Interaction, option):
        try:
            with PerformanceLogger(logger, f"handle_vote_{option}"):
                # Get the cog instance dynamically
                cog = self._get_cog(interaction)

                # Extract question_id from the message using mapping
                question_id = self.question_id
                if not question_id:
                    question_id = await cog.get_question_id_from_message(interaction.message.id)

                if not question_id:
                    logger.error(f"Could not determine question ID for vote from {interaction.user}")
                    await interaction.response.send_message("❌ Could not determine which question you're voting on.",
                                                            ephemeral=True)
                    return

                await cog.record_vote(question_id, interaction.user.id, option)

                option_text = "Option 1" if option == "option1" else "Option 2"
                embed = discord.Embed(
                    title="✅ Vote Recorded!",
                    description=f"You voted for **{option_text}**",
                    color=discord.Color.green()
                )
                embed.set_footer(text="Your vote has been saved • You can change your vote anytime")

                await interaction.response.send_message(embed=embed, ephemeral=True)
                logger.info(f"Vote successfully processed for {interaction.user} (ID: {interaction.user.id}): {option}")

        except Exception as e:
            logger.error(f"Error handling vote from {interaction.user} (ID: {interaction.user.id}): {e}", exc_info=True)
            try:
                await interaction.response.send_message(
                    "❌ There was an error recording your vote. Please try again.",
                    ephemeral=True
                )
            except:
                logger.error(f"Failed to send error message to {interaction.user}")


async def setup(bot):
    logger.info("Setting up WYR cog")
    try:
        await bot.add_cog(WYR(bot))
        logger.info("WYR cog successfully added to bot")
    except Exception as e:
        logger.error(f"Failed to setup WYR cog: {e}", exc_info=True)
        raise