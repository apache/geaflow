#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""
LDBC Test Data Generator for GeaFlow Issue #363

This script generates larger-scale test data based on LDBC schema patterns.
Scale: Approximately LDBC SF0.1 (1/10 of SF1)
- ~300 Person vertices (20x current)
- ~3000 edges (30x current)

This provides a middle ground for performance testing without requiring
the full LDBC SF1 dataset generation infrastructure.
"""

import random
import os
from datetime import datetime, timedelta

# Configuration
SCALE_FACTOR = 20  # 20x current data size
OUTPUT_DIR = "../src/test/resources/data_large"
BASE_PERSON_ID = 1100001
BASE_POST_ID = 1120001
BASE_COMMENT_ID = 1130001
BASE_FORUM_ID = 1150001

# Random seed for reproducibility
random.seed(42)

def ensure_output_dir():
    """Create output directory if it doesn't exist"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")

def generate_timestamp():
    """Generate random timestamp"""
    start = datetime(2020, 1, 1)
    end = datetime(2024, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return int((start + timedelta(days=random_days)).timestamp() * 1000)

def generate_persons(count):
    """Generate Person vertices"""
    print(f"Generating {count} Person vertices...")
    persons = []

    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry",
                   "Iris", "Jack", "Kate", "Leo", "Mary", "Nancy", "Oscar", "Peter",
                   "Queen", "Rose", "Sam", "Tom", "Uma", "Victor", "Wendy", "Xander",
                   "Yara", "Zoe"]

    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
                  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]

    genders = ["male", "female"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]

    for i in range(count):
        person_id = BASE_PERSON_ID + i
        creation_date = generate_timestamp()
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        gender = random.choice(genders)
        browser = random.choice(browsers)
        ip = f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"

        persons.append(f"{person_id}|Person|{creation_date}|{first_name}|{last_name}|{gender}|{browser}|{ip}")

    return persons

def generate_posts(person_count, posts_per_person=3):
    """Generate Post vertices"""
    total_posts = person_count * posts_per_person
    print(f"Generating {total_posts} Post vertices...")
    posts = []

    contents = [
        "Great discussion about graph databases!",
        "Learning GQL and finding it very powerful",
        "Excited about the new features in GeaFlow",
        "Performance optimization is key for large graphs",
        "Just finished implementing a complex query",
        "Graph algorithms are fascinating",
        "Working on a social network analysis project",
        "Impressed by the scalability of graph systems"
    ]

    languages = ["en", "zh", "es", "fr", "de"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge"]

    post_id = BASE_POST_ID
    for person_idx in range(person_count):
        for _ in range(random.randint(1, posts_per_person + 2)):
            creation_date = generate_timestamp()
            browser = random.choice(browsers)
            ip = f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
            content = random.choice(contents)
            length = len(content)
            lang = random.choice(languages)
            image = f"photo{random.randint(1, 100)}.jpg" if random.random() > 0.7 else ""

            posts.append(f"{post_id}|Post|{creation_date}|{browser}|{ip}|{content}|{length}|{lang}|{image}")
            post_id += 1

    return posts

def generate_comments(person_count, comments_per_person=2):
    """Generate Comment vertices"""
    total_comments = person_count * comments_per_person
    print(f"Generating {total_comments} Comment vertices...")
    comments = []

    contents = [
        "I agree with this point",
        "Interesting perspective!",
        "Thanks for sharing",
        "Could you elaborate more?",
        "Great explanation",
        "Very helpful information"
    ]

    browsers = ["Chrome", "Firefox", "Safari"]

    comment_id = BASE_COMMENT_ID
    for _ in range(total_comments):
        creation_date = generate_timestamp()
        browser = random.choice(browsers)
        ip = f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
        content = random.choice(contents)
        length = len(content)

        comments.append(f"{comment_id}|Comment|{creation_date}|{browser}|{ip}|{content}|{length}")
        comment_id += 1

    return comments

def generate_forums(count):
    """Generate Forum vertices"""
    print(f"Generating {count} Forum vertices...")
    forums = []

    titles = [
        "Graph Database Enthusiasts",
        "GQL Language Discussion",
        "Performance Optimization Tips",
        "Graph Algorithms Study Group",
        "Social Network Analysis",
        "Distributed Systems Forum",
        "Big Data Processing",
        "GeaFlow Users"
    ]

    for i in range(count):
        forum_id = BASE_FORUM_ID + i
        creation_date = generate_timestamp()
        title = f"{random.choice(titles)} #{i+1}"

        forums.append(f"{forum_id}|Forum|{creation_date}|{title}")

    return forums

def generate_knows_edges(person_count):
    """Generate knows relationships (Person-knows->Person)"""
    print(f"Generating knows edges...")
    edges = []

    # Each person knows 5-15 other persons
    for person_idx in range(person_count):
        person_id = BASE_PERSON_ID + person_idx
        num_knows = random.randint(5, 15)

        # Select random persons to know (avoid self)
        known_indices = random.sample([i for i in range(person_count) if i != person_idx],
                                     min(num_knows, person_count - 1))

        for known_idx in known_indices:
            known_id = BASE_PERSON_ID + known_idx
            creation_date = generate_timestamp()
            edges.append(f"{person_id}|{known_id}|knows|{creation_date}")

    return edges

def generate_has_creator_edges(posts, comments, person_count):
    """Generate hasCreator relationships (Post/Comment-hasCreator->Person)"""
    print(f"Generating hasCreator edges...")
    edges = []

    # Posts
    for post_line in posts:
        post_id = int(post_line.split('|')[0])
        creator_id = BASE_PERSON_ID + random.randint(0, person_count - 1)
        edges.append(f"{post_id}|{creator_id}|hasCreator")

    # Comments
    for comment_line in comments:
        comment_id = int(comment_line.split('|')[0])
        creator_id = BASE_PERSON_ID + random.randint(0, person_count - 1)
        edges.append(f"{comment_id}|{creator_id}|hasCreator")

    return edges

def generate_reply_of_edges(comments, posts):
    """Generate replyOf relationships (Comment-replyOf->Post)"""
    print(f"Generating replyOf edges...")
    edges = []

    for comment_line in comments:
        comment_id = int(comment_line.split('|')[0])
        # Randomly select a post to reply to
        if posts:
            post_line = random.choice(posts)
            post_id = int(post_line.split('|')[0])
            edges.append(f"{comment_id}|{post_id}|replyOf")

    return edges

def write_file(filename, lines):
    """Write lines to file"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w') as f:
        for line in lines:
            f.write(line + '\n')
    print(f"  Written {len(lines)} lines to {filename}")

def main():
    """Main data generation function"""
    print("=" * 60)
    print("GeaFlow LDBC Test Data Generator")
    print(f"Scale Factor: {SCALE_FACTOR}x")
    print("=" * 60)

    ensure_output_dir()

    # Calculate counts
    person_count = 15 * SCALE_FACTOR  # 300 persons

    # Generate vertices
    persons = generate_persons(person_count)
    posts = generate_posts(person_count, posts_per_person=3)
    comments = generate_comments(person_count, comments_per_person=2)
    forums = generate_forums(person_count // 30)  # ~10 forums

    # Generate edges
    knows_edges = generate_knows_edges(person_count)
    has_creator_edges = generate_has_creator_edges(posts, comments, person_count)
    reply_of_edges = generate_reply_of_edges(comments, posts)

    # Combine all edges
    all_edges = has_creator_edges + reply_of_edges
    all_edges_with_value = knows_edges

    # Write to files
    print("\nWriting files...")
    write_file("bi_person", persons)
    write_file("bi_post", posts)
    write_file("bi_comment", comments)
    write_file("bi_forum", forums)
    write_file("bi_edge", all_edges)
    write_file("bi_edge_with_value", all_edges_with_value)

    # Statistics
    print("\n" + "=" * 60)
    print("Data Generation Complete!")
    print("=" * 60)
    print(f"Persons:        {len(persons)}")
    print(f"Posts:          {len(posts)}")
    print(f"Comments:       {len(comments)}")
    print(f"Forums:         {len(forums)}")
    print(f"Edges:          {len(all_edges)}")
    print(f"Edges w/value:  {len(all_edges_with_value)}")
    print(f"Total edges:    {len(all_edges) + len(all_edges_with_value)}")
    print("=" * 60)

    # Generate Issue #363 specific IDs that exist in data
    print("\nFor Issue #363 Query:")
    print(f"  Person IDs: {BASE_PERSON_ID} to {BASE_PERSON_ID + person_count - 1}")
    print(f"  Suggested ID for testing: {BASE_PERSON_ID} and {BASE_PERSON_ID + person_count // 2}")
    print(f"  Post IDs: {BASE_POST_ID} to {posts[-1].split('|')[0]}")

if __name__ == "__main__":
    main()
