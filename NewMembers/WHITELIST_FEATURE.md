# Whitelist Feature Documentation

## Overview
The whitelist feature allows admins and moderators to bypass the account age restriction (90 days) for specific users. This is designed to allow trusted friends or known community members with new Discord accounts to join the server.

## Features Implemented

### 1. Database Collection
- **Collection**: `Whitelist` in `Server-Data` database
- **Indexes**: Optimized for fast lookups by guild, user, and username
- **Fields**:
  - `guild_id`: Server ID
  - `user_id`: Discord user ID (stored for security)
  - `username`: Discord username (case-sensitive, stored for identification)
  - `added_by`: User ID of admin who added them
  - `added_by_username`: Username of admin who added them
  - `added_at`: Timestamp of when added
  - `reason`: Required reason for whitelisting
  - `is_active`: Boolean flag for soft delete
  - `role_assigned`: Whether the special role is currently assigned
  - `role_assigned_at`: When the role was assigned
  - `account_age_at_join`: Age of account when they joined

### 2. Commands
All commands are under the `/whitelist` group and require `Manage Roles` permission or Admin/Moderator/Staff role.

#### `/whitelist add <user>`
- **Description**: Add a member to the whitelist
- **User Parameter**: Accepts either:
  - User ID (recommended, e.g., `1234567890123456789`)
  - Exact username (case-sensitive, e.g., `JohnDoe`)
- **Process**:
  1. Opens a modal requiring a reason (10-500 characters)
  2. Validates the user exists
  3. Stores both user ID and username for security
  4. If the user is in the server and has a new account, assigns the "Whitelisted New Member" role
- **Security**: Stores both ID and username to prevent impersonation via username changes

#### `/whitelist remove <user>`
- **Description**: Remove a member from the whitelist
- **User Parameter**: Same as add - user ID or exact username
- **Process**:
  1. Soft deletes the whitelist entry (marks as inactive)
  2. Removes the "Whitelisted New Member" role if assigned

#### `/whitelist list`
- **Description**: List all whitelisted members
- **Display**: Shows up to 25 entries with:
  - Username and user ID
  - Who added them and when
  - Reason for whitelisting
  - Whether role is assigned

#### `/whitelist check <user>`
- **Description**: Check if a specific member is whitelisted
- **User Parameter**: User ID or exact username
- **Display**: Shows detailed whitelist information if found

### 3. Special Role: "Whitelisted New Member"
- **Color**: Blue
- **Display**: Hoisted (displayed separately in member list)
- **Purpose**: Visually identify whitelisted members with new accounts
- **Auto-Assignment**: Assigned when:
  - User is added to whitelist AND
  - User is in the server AND
  - User's account is less than 90 days old
- **Auto-Removal**: Removed when:
  - User's account reaches 90 days old (checked hourly)
  - User is removed from whitelist
  - User leaves the server (marked as unassigned in database)

### 4. Member Join Flow
When a member joins:
1. Bot checks if member is a bot → skip processing
2. Bot checks account age
3. If account < 90 days old:
   - **NEW**: Check if user is in whitelist
   - If whitelisted:
     - Allow member to join
     - Assign "Whitelisted New Member" role
     - Send normal welcome message
     - Skip all kick logic
   - If NOT whitelisted:
     - Continue with normal age restriction kick logic

### 5. Background Task: Role Cleanup
- **Frequency**: Runs every hour
- **Process**:
  1. Finds all whitelisted members with assigned roles
  2. Checks their current account age
  3. If account ≥ 90 days old:
     - Removes the "Whitelisted New Member" role
     - Updates database (marks role as unassigned)
     - Sends a congratulatory DM to the member
- **Logging**: Logs all actions and errors for monitoring

## File Structure

```
NewMembers/
├── admin/
│   ├── welcometrigger.py          (existing)
│   └── whitelist.py                (NEW - whitelist commands)
├── tasks/
│   └── whitelist_role_cleanup.py   (NEW - auto role removal)
├── joining.py                      (MODIFIED - added whitelist check)
└── WHITELIST_FEATURE.md            (this file)

Database/database/
└── define_collections.py           (MODIFIED - added whitelist collection)
```

## Configuration
All configuration is in `NewMembers/admin/whitelist.py`:

```python
WHITELIST_ROLE_NAME = "Whitelisted New Member"
WHITELIST_ROLE_COLOR = discord.Color.blue()
ACCOUNT_AGE_REQUIREMENT_DAYS = 90  # Must match joining.py
```

## Security Features

### 1. Dual Storage (User ID + Username)
- Both user ID and username are stored
- Username alone could be exploited if someone changes their name
- User ID is the primary verification
- Username is for human readability and resolution

### 2. Case-Sensitive Username Matching
- Prevents case-based impersonation
- User must provide exact username or use user ID

### 3. Permission Checks
- Requires `Manage Roles` permission OR
- Admin/Moderator/Staff role

### 4. Audit Trail
- Tracks who added/removed entries
- Tracks timestamps
- Requires reason for all additions
- Logs all actions

### 5. Soft Delete
- Entries are marked inactive, not deleted
- Maintains historical record
- Can be reactivated

## Usage Examples

### Example 1: Add by User ID (Recommended)
```
/whitelist add user:1234567890123456789
→ Modal opens
→ Enter reason: "Friend of active member JaneDoe"
→ Success! User added to whitelist
```

### Example 2: Add by Username
```
/whitelist add user:JohnDoe
→ Must be exact case-sensitive username
→ Modal opens
→ Enter reason: "Known from other community"
→ Success! User added to whitelist
```

### Example 3: Check Status
```
/whitelist check user:JohnDoe
→ Shows:
  - Added by: @AdminName
  - Date: Jan 1, 2024 3:45 PM
  - Role Assigned: ✅ Yes
  - Reason: Friend of active member JaneDoe
```

### Example 4: List All
```
/whitelist list
→ Shows all whitelisted members with details
```

### Example 5: Remove
```
/whitelist remove user:JohnDoe
→ Success! User removed from whitelist
→ Role removed (if assigned)
```

## Testing Checklist

### Basic Functionality
- [ ] `/whitelist add` with user ID works
- [ ] `/whitelist add` with username works
- [ ] Modal requires reason (10-500 chars)
- [ ] `/whitelist list` shows entries
- [ ] `/whitelist check` finds entries
- [ ] `/whitelist remove` works

### Role Assignment
- [ ] Role is created on first use
- [ ] Role is assigned when whitelisted user joins (account < 90 days)
- [ ] Role is NOT assigned for accounts ≥ 90 days
- [ ] Role is removed when account ages out
- [ ] Role is removed when user is removed from whitelist

### Member Join Flow
- [ ] Whitelisted users with new accounts are allowed to join
- [ ] Non-whitelisted users with new accounts are kicked
- [ ] Normal welcome message is sent to whitelisted users
- [ ] Whitelisted users bypass age restriction

### Security
- [ ] Permission checks work (non-admins cannot use commands)
- [ ] Case-sensitive username matching works
- [ ] Username changes don't break ID verification
- [ ] Bots cannot be whitelisted

### Edge Cases
- [ ] User not found error handling
- [ ] Duplicate additions are prevented
- [ ] User leaving server is handled
- [ ] Background task error handling
- [ ] Database connection issues are handled

## Troubleshooting

### Issue: Commands not showing up
- **Solution**: Restart bot to sync slash commands
- **Check**: Run `python codex.py` and verify cog loading logs

### Issue: Role not being assigned
- **Check**: Bot has `Manage Roles` permission
- **Check**: Bot's role is higher than "Whitelisted New Member" role
- **Check**: Database connection is working
- **Check**: User's account age is < 90 days

### Issue: Background task not running
- **Check**: Cog loaded successfully (check logs)
- **Check**: No errors in background task logs
- **Verify**: Run `/whitelist list` to see if entries exist

### Issue: User ID resolution failing
- **Solution**: Use user ID instead of username
- **Verify**: Username is exact case match
- **Check**: User exists on Discord

## Monitoring

### Key Log Messages
```
# Successful whitelist add
"User {username} ({user_id}) added to whitelist by {admin} in guild {guild_name}"

# Whitelisted user joins
"Member {member} is whitelisted, bypassing age restriction (account age: X days)"

# Role assigned
"Assigned whitelist role to {member}"

# Role auto-removed
"Removed whitelist role from {member} (account age: X days)"

# Background task runs
"Whitelist role cleanup complete: Checked X, Removed Y, Errors Z"
```

### Database Queries
```python
# Check whitelist entries
await db_manager.get_collection_manager('serverdata_whitelist').find_many({'guild_id': GUILD_ID})

# Check active entries with roles
await db_manager.get_collection_manager('serverdata_whitelist').find_many({
    'guild_id': GUILD_ID,
    'is_active': True,
    'role_assigned': True
})
```

## Future Enhancements (Optional)

1. **Expiration**: Auto-expire whitelist entries after X days
2. **Bulk Import**: Import multiple users from a file
3. **Notifications**: Notify when whitelisted user joins
4. **Statistics**: Track whitelist usage metrics
5. **Appeal System**: Let kicked users request whitelist
6. **Trust Levels**: Different whitelist tiers with different permissions

## Notes

- The account age requirement (90 days) is hardcoded in multiple places and should be kept in sync
- The whitelist is server-specific (guild_id based)
- The background task runs every hour - adjust frequency in `whitelist_role_cleanup.py` if needed
- DM notifications are optional and gracefully fail if user has DMs disabled
- All database operations use the secondary connection pool ('Server-Data' database)
