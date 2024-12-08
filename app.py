import csv
import re
from traceback import print_stack
from pyparsing import Regex
import redis
# from redis.commands.search.field import TextField, NumericField, TagField
# from redis.commands.search.indexDefinition import IndexDefinition
# from redis.commands.search.query import Query
# https://redis.readthedocs.io/en/stable/examples.html
import sys

class Redis_Client():
    redis = None

    def __init__(self):
        self.redis = self.redis

    """
    Connect to redis with "host", "port", "db", "username" and "password".
    """
    def connect(self):
        try:
            self.redis = redis.Redis(
                host="redis-16863.c262.us-east-1-3.ec2.redns.redis-cloud.com",
                port=16863,
                decode_responses=True,
                username="default",
                password="4fWLo11TJsTtE7HCGjgnNoABKbuMmIjJ",
            )
            if self.redis.ping():
                print("Successfully connected to Redis Cloud!")
        except Exception as e:
            print(f"Connection failed: {e}")
            print_stack()

    def load_users(self, file):
        """
        Load the users dataset into Redis DB using pipeline for better performance.
        """
        print(f"Stared to load user Data users.txt")
        result = 0
        try:
            with open(file, 'r', encoding='utf-8') as f:
                pipe = self.redis.pipeline()
                for line in f:
                    # Spliting the lines into parts
                    parts = line.strip().split('" "')
                    if len(parts) < 13:  # Each user record should have at least 13 entries as per text line
                        print(f"Skipping malformed line: {line.strip()}")
                        continue

                    # Extracting user ID (first part) and attributes (remaining parts)
                    user_id = parts[0].strip('"')  # E.g., "user:1"
                    attributes = parts[1:]

                    # Creating a dictionary of attributes (key-value pairs)
                    user_dict = {}
                    for i in range(0, len(attributes), 2):
                        key = attributes[i].strip('"')  # Attribute name for each
                        value = attributes[i + 1].strip('"')  # Attribute value for each pair
                        user_dict[key] = value

                    # Adding the command to the pipeline
                    pipe.hset(user_id, mapping=user_dict)
                    result += 1

                    # Executing pipeline every 500 users
                    if result % 500 == 0:
                        pipe.execute()
                        print(f"Loaded {result} users so far...")

                # Executing any remaining commands in the pipeline
                pipe.execute()
            print(f"Loaded {result} users into Redis.")
        except UnicodeDecodeError as e:
            print(f"Error decoding file: {e}")
        except Exception as e:
            print(f"Error loading users: {e}")
        return result



    def load_scores(self, file):
        """
        Load the scores dataset into Redis DB and maintain leaderboards.
        """
        print(f"Stared to load userescore Data userscores.csv")
        result = 0
        try:
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                pipe = self.redis.pipeline()

                for row in reader:
                    user_id = row["user:id"]  # E.g., "user:1"
                    score = row["score"]
                    leaderboard = row["leaderboard"]

                    # Storing score and leaderboard information in a hash
                    pipe.hset(user_id, mapping={
                        "score": score,
                        "leaderboard": leaderboard
                    })

                    # Maintaiingn a sorted set for the leaderboard
                    pipe.zadd(f"leaderboard:{leaderboard}", {user_id: int(score)})

                    result += 1
                pipe.execute()
            print(f"Loaded {result} scores into Redis.")
        except Exception as e:
            print(f"Error loading scores: {e}")
        return result

    # """
    # Load the scores dataset into Redis DB.
    # """
    # def load_scores(self):  # leaderboards for users
    #     pipe = self.redis.pipeline()
    #     # open and read from file
    #     # TODO:
    #     result = pipe.execute()
    #     print("load data for scores")
    #     return result

    # """
    # Delete all users in the DB.
    # """
    # def delete_users(self, hashes):
    #     pipe = self.redis.pipeline()
    #     for hash in hashes:
    #         pipe.delete(hash)
    #     result = pipe.execute()
    #     return result

    # """
    # Erase everything in the DB.
    # """
    # def delete_all(self):
    #     self.redis.flushdb()

    def get_batch_of_users(self, count=10):
        """
        Retrieve a batch of user records from Redis.
        :param count: Number of records to fetch (default is 10).
        """
        print("Fetching a batch of user records.")
        try:
            # Get all keys matching the user pattern
            user_keys = self.redis.keys("user:*")
            
            if not user_keys:
                print("No user records found in Redis.")
                return []

            # Limit to the specified count
            user_keys = user_keys[:count]

            # Use pipeline to fetch data for multiple users efficiently
            pipe = self.redis.pipeline()
            for key in user_keys:
                pipe.hgetall(key)

            # Execute the pipeline
            users = pipe.execute()

            # Combine keys with their respective attributes for better context
            user_data = {key: user for key, user in zip(user_keys, users)}

            # Print and return the batch of user records
            print(f"Fetched {len(user_data)} user records.")
            return user_data
        except Exception as e:
            print(f"Error fetching batch of user records: {e}")
            return {}

    """
    Return all the attributes of the user by usr.
    """

    def query1(self, usr):
        print("Executing query 1.")
        try:
            # Construct the correct key format
            redis_key = f"user:{usr}"
            
            # Retrieve all attributes of the user from Redis
            result = self.redis.hgetall(redis_key)
            
            if not result:
                print(f"No attributes found for user: {usr}")
                return None

            # Print and return the result
            print(f"Attributes for user {usr}: {result}")
            return result
        except Exception as e:
            print(f"Error executing query1 for user {usr}: {e}")
            return None

    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    """

    def query2(self, usr):
        """
        Return the coordinate (longitude and latitude) of the user by the usr.
        """
        print("Executing query 2.")
        try:
            # Construct the correct key format
            redis_key = f"user:{usr}"
            
            # Retrieve the longitude and latitude of the user from Redis
            longitude = self.redis.hget(redis_key, "longitude")
            latitude = self.redis.hget(redis_key, "latitude")
            
            if not longitude or not latitude:
                print(f"No coordinates found for user: {usr}")
                return None

            # Combine longitude and latitude into a tuple
            coordinates = (longitude, latitude)

            # Print and return the result
            print(f"Coordinates for user {usr}: {coordinates}")
            return coordinates
        except Exception as e:
            print(f"Error executing query2 for user {usr}: {e}")
            return None

    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    We want to search for a subset of keyspace with the cursor at 1280.
    To avoid the searching of the entire keyspace, we only want to go through only a small number of
    elements per call.
    That is, we expect to only search through the subset of the keyspace, and then incrementally iterate the
    next keyspace only if needed.
    (https://redis.io/commands/scan/). You can test the scan query in the redis-cli.
    """
    def query3(self):
        """
        Get the keys and last names of the users whose ids do not start with an odd number.
        We search for a subset of the keyspace with the cursor starting at 1280.
        """
        print("Executing query 3.")
        try:
            cursor = 1280  # Start from cursor 1280
            match_pattern = "user:*"
            userids = []
            result_lastnames = []

            while cursor != 0:  # Continue until the cursor loops back to 0
                # Use SCAN with an increased count for fewer iterations
                cursor, keys = self.redis.scan(cursor=cursor, match=match_pattern, count=100)

                # Process the fetched keys
                for key in keys:
                    # Extract the numeric part of the user ID
                    user_id = key.split(":")[1]

                    # Check if the ID starts with an even number
                    if user_id[0] in "02468":  # IDs starting with even digits
                        last_name = self.redis.hget(key, "last_name")
                        if last_name:
                            userids.append(key)
                            result_lastnames.append(last_name)

                # Break the loop early if enough results are found
                if len(userids) >= 100:  # Arbitrary limit to stop early
                    break

            # Print and return the result
            print(f"Found {len(userids)} users whose IDs do not start with an odd number.")
            print("User IDs:", userids)
            print("Last Names:", result_lastnames)
            return userids, result_lastnames
        except Exception as e:
            print(f"Error executing query3: {e}")
            return [], []
    
    """
        Return the female users in China or Russia with a latitude between 40 and 46 by iterating over all keys.
    """
    
    def query4(self):
        """
        Return the female users in China or Russia with a latitude between 40 and 46 by iterating over all keys.
        """
        print("Executing query 4.")
        try:
            matching_users = []
            cursor = 0

            while True:
                # Scan all user keys incrementally
                cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=100)

                for key in keys:
                    # Fetch user data
                    user_data = self.redis.hgetall(key)

                    # Apply the filter conditions
                    if (
                        user_data.get("gender") == "female" and
                        user_data.get("country") in ["China", "Russia"] and
                        40 <= float(user_data.get("latitude", 0)) <= 46
                    ):
                        matching_users.append({
                            "key": key,
                            "first_name": user_data.get("first_name"),
                            "country": user_data.get("country"),
                            "latitude": user_data.get("latitude")
                        })

                # Break when cursor loops back to 0
                if cursor == 0:
                    break

            print(f"Found {len(matching_users)} matching users.")
            return matching_users
        except Exception as e:
            print(f"Error executing query4: {e}")
            return []

    """
    Get the email ids of the top 10 players (in terms of score) in leaderboard:2.
    """
    
    def query5(self):
        """
        Get the email ids of the top 10 players (in terms of score) in leaderboard:2.
        """
        print("Executing query 5.")
        try:
            # Retrieve top 10 players from leaderboard:2
            top_players = self.redis.zrevrange("leaderboard:2", 0, 9)
            
            if not top_players:
                print("No players found in leaderboard:2.")
                return []

            # Fetch email IDs of top players
            results = []
            for player_id in top_players:
                email = self.redis.hget(player_id, "email")
                if email:
                    results.append(email)

            # Print and return the result
            print(f"Top 10 email IDs in leaderboard:2: {results}")
            return results
        except Exception as e:
            print(f"Error executing query5: {e}")
            return []


# git@github.com:redis-developer/redis-datasets.git
rs = Redis_Client()
rs.connect()
rs.load_users("users.txt")
rs.load_scores("userscores.csv")
batch = rs.get_batch_of_users(10)
for user_key, user_data in batch.items():
    print(f"{user_key}: {user_data}")

rs.query1(299)
rs.query2(2836)
rs.query3()
rs.query4()
rs.query5()
