import random
import sys
import string
import math
from collections import defaultdict, deque # Added for BFS (qsp) and received articles
import os
import argparse
import enum
import time # For timing

ALIAS_MAP = {
    "add_person": "ap",
    "add_relation": "ar",
    "modify_relation": "mr",
    "add_tag": "at",
    "del_tag": "dt",
    "add_to_tag": "att",
    "del_from_tag": "dft",
    "query_value": "qv",
    "query_circle": "qci",
    "query_triple_sum": "qts",
    "query_tag_age_var": "qtav",
    "query_best_acquaintance": "qba",
    "load_network": "ln",
    # New commands
    "create_official_account": "coa",
    "delete_official_account": "doa",
    "contribute_article": "ca",
    "delete_article": "da",
    "follow_official_account": "foa",
    "query_shortest_path": "qsp",
    "query_best_contributor": "qbc",
    "query_received_articles": "qra",
    "query_tag_value_sum": "qtvs",
    "query_couple_sum": "qcs",
}
# Reverse map for easier lookup
INSTRUCTION_MAP = {v: k for k, v in ALIAS_MAP.items()}

# Parameter Constraints (from 评测机.txt)
AGE_RANGE = (1, 200)
VALUE_RANGE = (1, 200) # Relation value >= 1 for addRelation normal behavior
MVAL_RANGE = (-200, 200) # modifyRelation value
NAME_LENGTH_RANGE = (1, 10) # Reduced for readability
N_RANGE = (1, 200) # Reduced N for smaller test cases initially
TAG_PERSONS_LIMIT = 999 # From JML
ARTICLE_RECEIVED_LIMIT = 5 # From JML queryReceivedArticles

# Use a reasonable range for generated IDs to increase collision probability (from 评测机.txt)
ID_POOL_RANGE = (-150, 150) # Reduced ID pool for higher collision chance
TAG_ID_POOL_RANGE = (-150, 150) # Reduced TAG ID pool
ACCOUNT_ID_POOL_RANGE = (-150, 150) # New: Account ID pool
ARTICLE_ID_POOL_RANGE = (-150, 150) # New: Article ID pool

# --- Exception/Outcome Keys (from 评测机.txt) ---
OUTCOME_NORMAL = "normal"
# Maps internal target key to the actual exception class name string
GENERATOR_TARGET_OUTCOME_MAP = {
    ("ap", "EPI"): "EqualPersonIdException",
    ("ar", "PINF_id1"): "PersonIdNotFoundException",
    ("ar", "PINF_id2"): "PersonIdNotFoundException",
    ("ar", "ERE"): "EqualRelationException",
    ("mr", "PINF_id1"): "PersonIdNotFoundException",
    ("mr", "PINF_id2"): "PersonIdNotFoundException",
    ("mr", "EPI"): "EqualPersonIdException",
    ("mr", "RNF"): "RelationNotFoundException",
    ("at", "PINF"): "PersonIdNotFoundException",
    ("at", "ETI"): "EqualTagIdException",
    ("dt", "PINF"): "PersonIdNotFoundException",
    ("dt", "TINF"): "TagIdNotFoundException",
    ("att", "PINF_p1"): "PersonIdNotFoundException",
    ("att", "PINF_p2"): "PersonIdNotFoundException",
    ("att", "EPI_id1_eq_id2"): "EqualPersonIdException",
    ("att", "RNF"): "RelationNotFoundException",
    ("att", "TINF"): "TagIdNotFoundException",
    ("att", "EPI_in_tag"): "EqualPersonIdException",
    ("dft", "PINF_p1"): "PersonIdNotFoundException",
    ("dft", "PINF_p2"): "PersonIdNotFoundException",
    ("dft", "TINF"): "TagIdNotFoundException",
    ("dft", "PINF_not_in_tag"): "PersonIdNotFoundException",
    ("qv", "PINF_id1"): "PersonIdNotFoundException",
    ("qv", "PINF_id2"): "PersonIdNotFoundException",
    ("qv", "RNF"): "RelationNotFoundException",
    ("qci", "PINF_id1"): "PersonIdNotFoundException",
    ("qci", "PINF_id2"): "PersonIdNotFoundException",
    ("qtav", "PINF"): "PersonIdNotFoundException",
    ("qtav", "TINF"): "TagIdNotFoundException",
    ("qba", "PINF"): "PersonIdNotFoundException",
    ("qba", "ANE"): "AcquaintanceNotFoundException",
    ("coa", "PINF"): "PersonIdNotFoundException",
    ("coa", "EOAI"): "EqualOfficialAccountIdException",
    ("doa", "PINF"): "PersonIdNotFoundException",
    ("doa", "OAINF"): "OfficialAccountIdNotFoundException",
    ("doa", "DAPermissionDenied"): "DeleteOfficialAccountPermissionDeniedException",
    ("ca", "PINF"): "PersonIdNotFoundException",
    ("ca", "OAINF"): "OfficialAccountIdNotFoundException",
    ("ca", "EAI"): "EqualArticleIdException",
    ("ca", "ContributePermissionDenied"): "ContributePermissionDeniedException",
    ("da", "PINF"): "PersonIdNotFoundException",
    ("da", "OAINF"): "OfficialAccountIdNotFoundException",
    ("da", "AINF"): "ArticleIdNotFoundException",
    ("da", "DAPermissionDenied"): "DeleteArticlePermissionDeniedException",
    ("foa", "PINF"): "PersonIdNotFoundException",
    ("foa", "OAINF"): "OfficialAccountIdNotFoundException",
    ("foa", "EPI_follower"): "EqualPersonIdException",
    ("qsp", "PINF_id1"): "PersonIdNotFoundException",
    ("qsp", "PINF_id2"): "PersonIdNotFoundException",
    ("qsp", "PathNotFound"): "PathNotFoundException",
    ("qbc", "OAINF"): "OfficialAccountIdNotFoundException",
    ("qra", "PINF"): "PersonIdNotFoundException",
    ("qtvs", "PINF"): "PersonIdNotFoundException",
    ("qtvs", "TINF"): "TagIdNotFoundException",
}
ALL_TARGET_KEYS = list(GENERATOR_TARGET_OUTCOME_MAP.keys())
# Create a reverse map for easier lookup in _generate_arguments
EXCEPTION_NAME_TO_TARGET_KEY = defaultdict(list)
for key, name in GENERATOR_TARGET_OUTCOME_MAP.items():
    EXCEPTION_NAME_TO_TARGET_KEY[name].append(key)


# --- Constants from the Original DataGenerator ---
PUBLIC_MAX_INSTRUCTIONS = 10000
PUBLIC_MAX_N_LOAD = 300
MUTUAL_MAX_INSTRUCTIONS = 3000
MUTUAL_MAX_N_LOAD = 100

# --- Command List (from Original DataGenerator, align with ALIAS_MAP) ---
COMMANDS = set(ALIAS_MAP.values()) - {'ln'} # Exclude 'ln' as it's handled separately
LOAD_CMDS = {"ln"} # Only 'ln' now, 'lnl' wasn't in 评测机.txt
COMMANDS_WITH_ARGS = COMMANDS # All non-load commands need args

# --- Exceptions By Command (derived from GENERATOR_TARGET_OUTCOME_MAP) ---
EXCEPTION_MAP = defaultdict(set)
for (cmd_alias, _), exc_name in GENERATOR_TARGET_OUTCOME_MAP.items():
    EXCEPTION_MAP[cmd_alias].add(exc_name)

# --- Generation Phases (from Original DataGenerator) ---
class GenPhase(enum.Enum):
    INITIAL_LOAD = 0
    BUILD_NETWORK = 1
    RANDOM_MIX = 2
    STRESS_COMPLEX = 3

# --- Helper (from Original DataGenerator) ---
# Keep random_string if needed, or use the one from 评测机.txt logic
# def random_string(max_length=MAX_STRING_LEN):
#     length = random.randint(1, max(1, max_length))
#     return ''.join(random.choice(string.ascii_letters) for _ in range(length))

# --- Generator Class ---
class DataGenerator:
    def __init__(self, mode='P', num_logical_instructions=100):
        self.mode = mode.upper()
        self.target_instructions = num_logical_instructions
        if self.mode == 'P':
            self.max_instr_limit = PUBLIC_MAX_INSTRUCTIONS
            self.max_n_load_limit = PUBLIC_MAX_N_LOAD # Max N for ln
        else:
            self.max_instr_limit = MUTUAL_MAX_INSTRUCTIONS
            self.max_n_load_limit = MUTUAL_MAX_N_LOAD # Max N for ln
        self.target_instructions = min(self.target_instructions, self.max_instr_limit)
        self._initialize_state() # Initialize state here

    def _initialize_state(self):
        """Initializes state based on 评测机.txt's structure."""
        self.network_state = {
            "persons": {},
            "person_tags": {},
            "tag_members": {},
            "relations": {},
            "accounts": {},
            "articles_map": {},
            "received_articles": {},
            "triple_sum": 0,
            "couple_sum_dirty": True,
        }
        # Generation tracking
        self.instructions_generated = 0
        self.generated_lines = []
        self.current_phase = GenPhase.INITIAL_LOAD
        self.phase_instruction_count = 0
        # --- Guarantee Tracking ---
        self.commands_successfully_generated = set() # Tracks command ALIASES
        self.exceptions_attempted = set() # Tracks TUPLE target_keys
        self.all_exceptions_to_attempt = set(ALL_TARGET_KEYS) # Set of TUPLE target_keys

    # --- State Query Helper Functions (Ported from 评测机.txt as methods) ---
    def _get_existing_person_ids(self):
        return list(self.network_state["persons"].keys())

    def _get_existing_account_ids(self):
        return list(self.network_state["accounts"].keys())

    def _get_existing_article_ids(self):
        return list(self.network_state["articles_map"].keys())

    def _get_existing_relation_pairs(self):
        return list(self.network_state["relations"].keys())

    def _get_existing_tag_ids_for_person(self, person_id):
        return list(self.network_state["person_tags"].get(person_id, set()))

    def _get_persons_in_tag(self, owner_id, tag_id):
        return list(self.network_state["tag_members"].get((owner_id, tag_id), {}).keys())

    def _get_accounts_owned_by_person(self, person_id):
        return [acc_id for acc_id, acc_data in self.network_state["accounts"].items() if acc_data["owner_id"] == person_id]

    def _get_followers_of_account(self, account_id):
        acc_data = self.network_state["accounts"].get(account_id)
        return list(acc_data["followers"].keys()) if acc_data else []

    def _get_articles_of_account(self, account_id):
        acc_data = self.network_state["accounts"].get(account_id)
        return list(acc_data["articles"]) if acc_data else []

    def _bfs_reachable(self, start_id):
        """Finds all persons reachable from start_id in the network state using BFS."""
        state = self.network_state
        if start_id not in state["persons"]:
            return set()
        reachable = set()
        queue = deque([start_id])
        visited = {start_id}
        reachable.add(start_id)
        while queue:
            current_id = queue.popleft()
            current_person = state["persons"].get(current_id)
            if current_person and "acquaintances" in current_person:
                for acquaintance_id in current_person["acquaintances"].keys():
                    if acquaintance_id not in visited:
                        visited.add(acquaintance_id)
                        reachable.add(acquaintance_id)
                        queue.append(acquaintance_id)
        return reachable

    # --- Helper Functions for Parameter Generation (Ported from 评测机.txt as methods) ---
    def _generate_random_id(self, id_type="person", used_ids=None):
        if id_type == "person": pool_range = ID_POOL_RANGE
        elif id_type == "account": pool_range = ACCOUNT_ID_POOL_RANGE
        elif id_type == "article": pool_range = ARTICLE_ID_POOL_RANGE
        elif id_type == "tag": pool_range = TAG_ID_POOL_RANGE
        else: raise ValueError(f"Unknown ID type: {id_type}")
        id_pool = set(range(pool_range[0], pool_range[1] + 1))
        if used_ids is not None:
            available_ids = list(id_pool - set(used_ids))
            if available_ids:
                return random.choice(available_ids)
        return random.randint(pool_range[0], pool_range[1])

    def _get_random_non_existent_id(self, id_type="person"):
        if id_type == "person":
            existing_ids = self._get_existing_person_ids()
            pool_range = ID_POOL_RANGE
        elif id_type == "account":
            existing_ids = self._get_existing_account_ids()
            pool_range = ACCOUNT_ID_POOL_RANGE
        elif id_type == "article":
            existing_ids = self._get_existing_article_ids()
            pool_range = ARTICLE_ID_POOL_RANGE
        elif id_type == "tag":
             return None # This helper is not suitable for tags (needs person context)
        else: raise ValueError(f"Unknown ID type: {id_type}")
        all_possible_ids = set(range(pool_range[0], pool_range[1] + 1))
        existing_ids_set = set(existing_ids)
        available_ids = list(all_possible_ids - existing_ids_set)
        return random.choice(available_ids) if available_ids else None

    def _get_random_non_existent_tag_id_for_person(self, person_id):
        existing_tag_ids = self._get_existing_tag_ids_for_person(person_id)
        tag_pool = set(range(TAG_ID_POOL_RANGE[0], TAG_ID_POOL_RANGE[1] + 1))
        available_tag_ids = list(tag_pool - set(existing_tag_ids))
        return random.choice(available_tag_ids) if available_tag_ids else None

    def _generate_random_name(self, length_range=NAME_LENGTH_RANGE):
        length = random.randint(length_range[0], length_range[1])
        if length <= 0: length = 1
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def _generate_random_age(self, age_range=AGE_RANGE):
        return random.randint(age_range[0], age_range[1])

    def _generate_random_value(self, value_range=VALUE_RANGE):
        return random.randint(value_range[0], value_range[1])

    def _generate_random_mval(self, mval_range=MVAL_RANGE):
        return random.randint(mval_range[0], mval_range[1])

    def _get_random_existing_person_id(self):
        existing_ids = self._get_existing_person_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_account_id(self):
        existing_ids = self._get_existing_account_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_article_id(self):
         existing_ids = self._get_existing_article_ids()
         return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_tag_id_for_person(self, person_id):
         existing_tag_ids = self._get_existing_tag_ids_for_person(person_id)
         return random.choice(existing_tag_ids) if existing_tag_ids else None

    def _get_random_person_in_tag(self, owner_id, tag_id):
         member_ids = self._get_persons_in_tag(owner_id, tag_id)
         return random.choice(member_ids) if member_ids else None

    def _get_random_person_not_in_tag(self, owner_id, tag_id):
         existing_person_ids = self._get_existing_person_ids()
         members_in_tag = set(self._get_persons_in_tag(owner_id, tag_id))
         persons_not_in_tag = [pid for pid in existing_person_ids if pid not in members_in_tag]
         return random.choice(persons_not_in_tag) if persons_not_in_tag else None

    def _get_random_account_owned_by_person(self, person_id):
        owned_accounts = self._get_accounts_owned_by_person(person_id)
        return random.choice(owned_accounts) if owned_accounts else None

    def _get_random_account_not_owned_by_person(self, person_id):
        existing_account_ids = self._get_existing_account_ids()
        non_owned_accounts = [acc_id for acc_id in existing_account_ids if self.network_state["accounts"][acc_id]["owner_id"] != person_id]
        return random.choice(non_owned_accounts) if non_owned_accounts else None

    def _get_random_follower_of_account(self, account_id):
        followers = self._get_followers_of_account(account_id)
        return random.choice(followers) if followers else None

    def _get_random_non_follower_of_account(self, account_id):
        existing_person_ids = self._get_existing_person_ids()
        followers = set(self._get_followers_of_account(account_id))
        non_followers = [pid for pid in existing_person_ids if pid not in followers]
        return random.choice(non_followers) if non_followers else None

    def _get_random_article_of_account(self, account_id):
        articles = self._get_articles_of_account(account_id)
        return random.choice(articles) if articles else None

    def _get_random_article_not_of_account(self, account_id):
         existing_articles = self._get_existing_article_ids()
         account_articles = set(self._get_articles_of_account(account_id))
         non_account_articles = [aid for aid in existing_articles if aid not in account_articles]
         return random.choice(non_account_articles) if non_account_articles else None

    # --- Command Generators (Ported from 评测机.txt as private methods) ---
    # Each returns (cmd_str, params, outcome_string) or (None, None, None)
    # They now access state via self.network_state and helpers via self._...

    def _generate_ap(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        _id = None
        params = {}
        outcome = None

        if target_key == ("ap", "EPI"):
            if existing_ids:
                _id = random.choice(existing_ids)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            _id = self._get_random_non_existent_id("person")
            if _id is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else: return None, None, None

        if _id is None: return None, None, None

        name = self._generate_random_name()
        age = self._generate_random_age()
        params = {"id": _id, "name": name, "age": age}
        cmd_str = f"ap {_id} {name} {age}"
        return cmd_str, params, outcome

    def _generate_ar(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        params = {}
        outcome = None

        if target_key and target_key[0] == "ar" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) == 0: return None, None, None

            if target_key == ("ar", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None: return None, None, None
            elif target_key == ("ar", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("ar", "EPI"): # EPI check moved to JML, this generates id1=id2 case
             if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = id1
                # Even though JML handles EPI, we might want to test it specifically
                # Let's assume the target_key implies we *want* id1 == id2 for testing input parsing etc.
                # But the *expected* outcome according to JML is PINF if id doesn't exist, or normal/ERE if it does.
                # This needs clarification. Let's keep generating id1=id2 for now assuming the test harness handles the expected JML outcome.
                # If the intention is *only* to generate valid normal/exception cases *according to JML rules*, id1=id2 should likely not be generated here.
                # Revisit: For now, let's generate id1=id2 if requested by target key, but outcome might be debated.
                # Let's map it to PINF if one is non-existent, or assume it's normal/ERE otherwise.
                # The original 评测机 code mapped ("ar", "EPI") to EqualPersonIdException, which ar does NOT throw.
                # Let's remove ("ar", "EPI") from generation possibility as it's not a direct JML exception for 'ar'.
                return None, None, None # AR does not throw EPI directly

        elif target_key == ("ar", "ERE"):
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = OUTCOME_NORMAL
            # If few people, try adding new ones and linking them
            elif len(existing_ids) < 2 and self.instructions_generated < self.target_instructions - 5: # Check if we can add more people
                res1 = self._generate_ap(target_key=None)
                if res1: self._update_state_ap(res1[1]) # Add person 1
                res2 = self._generate_ap(target_key=None)
                if res2: self._update_state_ap(res2[1]) # Add person 2
                if res1 and res2:
                    id1, id2 = res1[1]['id'], res2[1]['id']
                    outcome = OUTCOME_NORMAL # Now try normal again
                else: return None, None, None # Couldn't add people
            else: return None, None, None # Cannot generate normal AR
        else: return None, None, None

        if id1 is None or id2 is None: return None, None, None
        if id1 == id2: return None, None, None # Ensure distinct IDs for normal/ERE/PINF cases

        value = self._generate_random_value()
        params = {"id1": id1, "id2": id2, "value": value}
        cmd_str = f"ar {id1} {id2} {value}"
        return cmd_str, params, outcome

    def _generate_mr(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        params = {}
        outcome = None

        if target_key and target_key[0] == "mr" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) < 1: return None, None, None # Need at least one existing for PINF

            if target_key == ("mr", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person") # Need another ID
                if id2 is None or id1 == id2: return None, None, None
            elif target_key == ("mr", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None or id1 == id2: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("mr", "EPI"):
            if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = id1
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("mr", "RNF"):
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None: return None, None, None

        m_val = self._generate_random_mval()
        params = {"id1": id1, "id2": id2, "m_val": m_val}
        cmd_str = f"mr {id1} {id2} {m_val}"
        return cmd_str, params, outcome

    def _generate_at(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        person_id = None
        tag_id = None
        params = {}
        outcome = None

        if target_key == ("at", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            tag_id = self._generate_random_id("tag")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("at", "ETI"):
            persons_with_tags = [pid for pid in existing_ids if self._get_existing_tag_ids_for_person(pid)]
            if persons_with_tags:
                person_id = random.choice(persons_with_tags)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None # Should not happen if persons_with_tags is not empty
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            if existing_ids:
                person_id = random.choice(existing_ids)
                # Try multiple times to find a non-existent tag ID
                for _ in range(10):
                    tag_id = self._get_random_non_existent_tag_id_for_person(person_id)
                    if tag_id is not None:
                        break
                else: # After attempts, generate a new one if pool failed
                    tag_id = self._generate_random_id("tag", used_ids=self._get_existing_tag_ids_for_person(person_id))

                if tag_id is None: return None, None, None # Cannot find non-existent tag for this person
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or tag_id is None: return None, None, None

        params = {"person_id": person_id, "tag_id": tag_id}
        cmd_str = f"at {person_id} {tag_id}"
        return cmd_str, params, outcome

    def _generate_dt(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        person_id = None
        tag_id = None
        params = {}
        outcome = None

        if target_key == ("dt", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            tag_id = self._generate_random_id("tag")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("dt", "TINF"):
            if existing_ids:
                person_id = random.choice(existing_ids)
                tag_id = self._get_random_non_existent_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None # Cannot find non-existent tag
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            persons_with_tags = [pid for pid in existing_ids if self._get_existing_tag_ids_for_person(pid)]
            if persons_with_tags:
                person_id = random.choice(persons_with_tags)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or tag_id is None: return None, None, None

        params = {"person_id": person_id, "tag_id": tag_id}
        cmd_str = f"dt {person_id} {tag_id}"
        return cmd_str, params, outcome

    def _generate_att(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        tag_id = None
        params = {}
        outcome = None

        if target_key and target_key[0] == "att" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) == 0: return None, None, None

            if target_key == ("att", "PINF_p1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None or id1 == id2: return None, None, None
            elif target_key == ("att", "PINF_p2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None or id1 == id2: return None, None, None
            tag_id = self._generate_random_id("tag")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("att", "EPI_id1_eq_id2"):
            if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = id1
                tag_id = self._generate_random_id("tag")
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("att", "RNF"):
            valid_attempts = []
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if i != j and (min(i, j), max(i, j)) not in state["relations"]]
            random.shuffle(non_linked_pairs)
            for p1_id, p2_id in non_linked_pairs:
                 p2_tags = self._get_existing_tag_ids_for_person(p2_id)
                 if p2_tags:
                      tag_id = random.choice(list(p2_tags))
                      valid_attempts.append((p1_id, p2_id, tag_id))
                      break # Found one
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("att", "TINF"):
            valid_attempts = []
            linked_pairs = list(state["relations"].keys())
            random.shuffle(linked_pairs)
            for min_id, max_id in linked_pairs:
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                if id1 == id2: continue
                tag_id = self._get_random_non_existent_tag_id_for_person(id2)
                if tag_id is not None:
                    valid_attempts.append((id1, id2, tag_id))
                    break # Found one
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("att", "EPI_in_tag"):
            valid_attempts = []
            tag_members_items = list(state["tag_members"].items())
            random.shuffle(tag_members_items)
            for (owner_id, tid), members in tag_members_items:
                p2_id = owner_id
                if p2_id not in state["persons"]: continue
                members_list = list(members.keys())
                random.shuffle(members_list)
                for p1_id in members_list:
                    if p1_id not in state["persons"]: continue
                    if p1_id != p2_id and (min(p1_id, p2_id), max(p1_id, p2_id)) in state["relations"]:
                         valid_attempts.append((p1_id, p2_id, tid))
                         break # Found one for this tag
                if valid_attempts: break # Found one overall
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            valid_attempts = []
            linked_pairs = list(state["relations"].keys())
            random.shuffle(linked_pairs)
            for min_id, max_id in linked_pairs:
                 id1_cand, id2_cand = random.choice([(min_id, max_id), (max_id, min_id)])
                 if id1_cand == id2_cand: continue
                 p2_tags = self._get_existing_tag_ids_for_person(id2_cand)
                 random.shuffle(p2_tags)
                 for tag_id_cand in p2_tags:
                     tag_key = (id2_cand, tag_id_cand)
                     tag_current_members = state["tag_members"].get(tag_key, {})
                     if id1_cand not in tag_current_members and len(tag_current_members) < TAG_PERSONS_LIMIT:
                         valid_attempts.append((id1_cand, id2_cand, tag_id_cand))
                         break # Found valid tag for this pair
                 if valid_attempts: break # Found valid pair and tag overall
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None or tag_id is None: return None, None, None

        params = {"id1": id1, "id2": id2, "tag_id": tag_id}
        cmd_str = f"att {id1} {id2} {tag_id}"
        return cmd_str, params, outcome

    def _generate_dft(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        tag_id = None
        params = {}
        outcome = None

        if target_key and target_key[0] == "dft" and "PINF" in target_key[1]:
             if len(existing_ids) < 1: return None, None, None

             if target_key == ("dft", "PINF_p1"):
                 non_existent_id = self._get_random_non_existent_id("person")
                 if non_existent_id is None: return None, None, None
                 id1 = non_existent_id
                 id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                 if id2 is None or id1 == id2: return None, None, None
                 tag_id = self._generate_random_id("tag")
                 outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
             elif target_key == ("dft", "PINF_p2"):
                 non_existent_id = self._get_random_non_existent_id("person")
                 if non_existent_id is None: return None, None, None
                 id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                 id2 = non_existent_id
                 if id1 is None or id1 == id2: return None, None, None
                 tag_id = self._generate_random_id("tag")
                 outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
             elif target_key == ("dft", "PINF_not_in_tag"):
                 valid_attempts = []
                 persons_with_tags = [(pid, tid) for pid in existing_ids for tid in self._get_existing_tag_ids_for_person(pid)]
                 random.shuffle(persons_with_tags)
                 for p2_id, tid in persons_with_tags:
                     tag_key = (p2_id, tid)
                     persons_not_in_tag_members = [pid for pid in existing_ids if pid != p2_id and pid not in state["tag_members"].get(tag_key, {})]
                     if persons_not_in_tag_members:
                         id1 = random.choice(persons_not_in_tag_members)
                         id2 = p2_id
                         tag_id = tid
                         valid_attempts.append((id1, id2, tag_id))
                         break # Found one
                 if valid_attempts:
                     id1, id2, tag_id = valid_attempts[0]
                     outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
                 else: return None, None, None
             else: return None, None, None

        elif target_key == ("dft", "TINF"):
            if len(existing_ids) < 1: return None, None, None
            id1 = random.choice(existing_ids)
            id2 = random.choice(existing_ids)
            tag_id = self._get_random_non_existent_tag_id_for_person(id2)
            if tag_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None: # Try normal
            valid_attempts = []
            tag_members_items = list(state["tag_members"].items())
            random.shuffle(tag_members_items)
            for (owner_id, tid), members in tag_members_items:
                p2_id = owner_id
                if members:
                    p1_id = random.choice(list(members.keys()))
                    valid_attempts.append((p1_id, p2_id, tid))
                    break # Found one
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None or tag_id is None: return None, None, None

        params = {"id1": id1, "id2": id2, "tag_id": tag_id}
        cmd_str = f"dft {id1} {id2} {tag_id}"
        return cmd_str, params, outcome

    def _generate_qv(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        params = {}
        outcome = None

        if target_key and target_key[0] == "qv" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) < 1: return None, None, None

            if target_key == ("qv", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None or id1 == id2: return None, None, None
            elif target_key == ("qv", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None or id1 == id2: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("qv", "RNF"):
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None: return None, None, None

        params = {"id1": id1, "id2": id2}
        cmd_str = f"qv {id1} {id2}"
        return cmd_str, params, outcome

    def _generate_qci(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        params = {}
        outcome = None

        if target_key and target_key[0] == "qci" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) < 1: return None, None, None

            if target_key == ("qci", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None: return None, None, None # id1==id2 is fine for qci
            elif target_key == ("qci", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None # id1==id2 is fine for qci
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None: # Try normal
            if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = random.choice(existing_ids) # Can be same
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None: return None, None, None

        params = {"id1": id1, "id2": id2}
        cmd_str = f"qci {id1} {id2}"
        return cmd_str, params, outcome

    def _generate_qts(self, target_key=None):
        if target_key is not None: return None, None, None
        cmd_str = "qts"
        params = {}
        outcome = OUTCOME_NORMAL
        return cmd_str, params, outcome

    def _generate_qtav(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        person_id = None
        tag_id = None
        params = {}
        outcome = None

        if target_key == ("qtav", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            tag_id = self._generate_random_id("tag")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("qtav", "TINF"):
            if existing_ids:
                person_id = random.choice(existing_ids)
                tag_id = self._get_random_non_existent_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            persons_with_tags = [pid for pid in existing_ids if self._get_existing_tag_ids_for_person(pid)]
            if persons_with_tags:
                person_id = random.choice(persons_with_tags)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                # QTAV specific: Ensure tag has members for meaningful result
                if not self._get_persons_in_tag(person_id, tag_id):
                    return None, None, None # Cannot generate normal if tag is empty
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or tag_id is None: return None, None, None

        params = {"person_id": person_id, "tag_id": tag_id}
        cmd_str = f"qtav {person_id} {tag_id}"
        return cmd_str, params, outcome

    def _generate_qba(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        _id = None
        params = {}
        outcome = None

        if target_key == ("qba", "PINF"):
            _id = self._get_random_non_existent_id("person")
            if _id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("qba", "ANE"):
            persons_with_no_acquaintances = [pid for pid, data in state["persons"].items() if not data.get("acquaintances", {})]
            if persons_with_no_acquaintances:
                _id = random.choice(persons_with_no_acquaintances)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            persons_with_acquaintances = [pid for pid, data in state["persons"].items() if data.get("acquaintances", {})]
            if persons_with_acquaintances:
                _id = random.choice(persons_with_acquaintances)
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if _id is None: return None, None, None

        params = {"id": _id}
        cmd_str = f"qba {_id}"
        return cmd_str, params, outcome

    def _generate_ln(self, target_key=None):
        # ln has no exceptions defined in JML requires, and no parameters beyond N and data.
        if target_key is not None: return None, None, None
        state = self.network_state # Get current state reference

        # Reset state (as ln does) - This generator will be called ONCE at the start.
        self._initialize_state()
        state = self.network_state # Get the *newly initialized* state reference

        # Determine N based on limits
        max_n = self.max_n_load_limit
        min_n = 1 if max_n > 0 else 0 # Ensure min_n is valid

        # Adjust N range based on ID pool size
        id_pool_size = ID_POOL_RANGE[1] - ID_POOL_RANGE[0] + 1
        if max_n > id_pool_size:
             # print(f"Warning: ID pool range ({id_pool_size}) too small for max N ({max_n}). Clamping max N.", file=sys.stderr)
             max_n = id_pool_size
             if min_n > max_n : min_n = max_n


        if max_n <= 0: # If pool size is 0 or negative range
            n = 0
        else:
             # Ensure a reasonable number, maybe tied to target instructions slightly?
             target_n = max(min_n, min(max_n, self.target_instructions // 10)) # Heuristic
             n = random.randint(min_n, max(min_n, target_n))


        ids = []
        if n > 0:
            id_pool = list(range(ID_POOL_RANGE[0], ID_POOL_RANGE[1] + 1))
            if len(id_pool) < n:
                 # print(f"Warning: ID pool size ({len(id_pool)}) less than requested N ({n}). Adjusting N to {len(id_pool)}.", file=sys.stderr)
                 n = len(id_pool)

            if n > 0:
                ids = random.sample(id_pool, n)

        names = [self._generate_random_name() for _ in range(n)]
        ages = [self._generate_random_age() for _ in range(n)]

        values_matrix = []
        if n > 1:
            for i in range(n - 1): # 0 to n-2
                row = [random.randint(0, VALUE_RANGE[1]) for _ in range(i + 1)]
                values_matrix.append(row)

        output_lines = [f"ln {n}"]
        output_lines.append(" ".join(map(str, ids)) if n > 0 else "")
        output_lines.append(" ".join(names) if n > 0 else "")
        output_lines.append(" ".join(map(str, ages)) if n > 0 else "")
        if n > 1:
             for row in values_matrix:
                output_lines.append(" ".join(map(str, row)))
        output_str = "\n".join(line for line in output_lines if line is not None) # Handle empty lines for n=0


        # Update state based on ln (critical!)
        for i in range(n):
            person_id = ids[i]
            state["persons"][person_id] = {"name": names[i], "age": ages[i], "acquaintances": {}}
            state["person_tags"][person_id] = set()
            state["received_articles"][person_id] = deque()

        if n > 1:
            for i in range(n - 1):
                for j in range(i + 1):
                    id1 = ids[i + 1]
                    id2 = ids[j]
                    value = values_matrix[i][j]
                    if value > 0:
                         state["persons"][id1]["acquaintances"][id2] = value
                         state["persons"][id2]["acquaintances"][id1] = value
                         state["relations"][(min(id1, id2), max(id1, id2))] = value

        state["triple_sum"] = 0 # Reset and recalculate
        state["couple_sum_dirty"] = True

        person_list = list(state["persons"].keys())
        for i in range(len(person_list)):
            for j in range(i + 1, len(person_list)):
                for k in range(j + 1, len(person_list)):
                     p1_id, p2_id, p3_id = person_list[i], person_list[j], person_list[k]
                     if (min(p1_id, p2_id), max(p1_id, p2_id)) in state["relations"] and \
                        (min(p2_id, p3_id), max(p2_id, p3_id)) in state["relations"] and \
                        (min(p3_id, p1_id), max(p3_id, p1_id)) in state["relations"]:
                         state["triple_sum"] += 1

        params = {"n": n, "ids": ids, "names": names, "ages": ages, "values_matrix": values_matrix}
        # NOTE: The state update is DONE HERE. The caller does not need to update state for ln.
        return output_str, params, OUTCOME_NORMAL

    # --- New Command Generators (Ported) ---
    def _generate_coa(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
        person_id = None
        account_id = None
        params = {}
        outcome = None

        if target_key == ("coa", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            account_id = self._generate_random_id("account")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("coa", "EOAI"):
            if existing_persons and existing_accounts:
                person_id = random.choice(existing_persons)
                account_id = random.choice(existing_accounts)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                if account_id is None: return None, None, None
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or account_id is None: return None, None, None

        account_name = self._generate_random_name()
        params = {"person_id": person_id, "account_id": account_id, "account_name": account_name}
        cmd_str = f"coa {person_id} {account_id} {account_name}"
        return cmd_str, params, outcome

    def _generate_doa(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
        person_id = None
        account_id = None
        params = {}
        outcome = None

        if target_key == ("doa", "PINF"):
            non_existent_person = self._get_random_non_existent_id("person")
            if non_existent_person is None: return None, None, None
            person_id = non_existent_person
            account_id = random.choice(existing_accounts) if existing_accounts else self._generate_random_id("account")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("doa", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                if account_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("doa", "DAPermissionDenied"):
            valid_attempts = []
            accounts_items = list(state["accounts"].items())
            random.shuffle(accounts_items)
            for acc_id, acc_data in accounts_items:
                owner_id = acc_data["owner_id"]
                non_owners = [pid for pid in existing_persons if pid != owner_id]
                if non_owners:
                    person_id = random.choice(non_owners)
                    account_id = acc_id
                    valid_attempts.append((person_id, account_id))
                    break # Found one
            if valid_attempts:
                person_id, account_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            valid_attempts = []
            persons_owning_accounts = [pid for pid in existing_persons if self._get_accounts_owned_by_person(pid)]
            if persons_owning_accounts:
                 person_id = random.choice(persons_owning_accounts)
                 owned_accounts = self._get_accounts_owned_by_person(person_id)
                 if owned_accounts:
                      account_id = random.choice(owned_accounts)
                      valid_attempts.append((person_id, account_id))
            if valid_attempts:
                person_id, account_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or account_id is None: return None, None, None

        params = {"person_id": person_id, "account_id": account_id}
        cmd_str = f"doa {person_id} {account_id}"
        return cmd_str, params, outcome

    def _generate_ca(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
        existing_articles = self._get_existing_article_ids()
        person_id = None
        account_id = None
        article_id = None
        params = {}
        outcome = None

        if target_key == ("ca", "PINF"):
            non_existent_person = self._get_random_non_existent_id("person")
            if non_existent_person is None: return None, None, None
            person_id = non_existent_person
            account_id = random.choice(existing_accounts) if existing_accounts else self._generate_random_id("account")
            article_id = self._generate_random_id("article")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("ca", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                article_id = self._generate_random_id("article")
                if account_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("ca", "EAI"):
            valid_attempts = []
            accounts_with_followers_and_articles = [
                acc_id for acc_id in existing_accounts
                if self._get_followers_of_account(acc_id) and self._get_articles_of_account(acc_id)
            ]
            if accounts_with_followers_and_articles:
                account_id = random.choice(accounts_with_followers_and_articles)
                followers = self._get_followers_of_account(account_id)
                articles_of_account = self._get_articles_of_account(account_id)
                if followers and articles_of_account: # Check again just in case
                    person_id = random.choice(followers)
                    article_id = random.choice(articles_of_account)
                    valid_attempts.append((person_id, account_id, article_id))

            if valid_attempts:
                person_id, account_id, article_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("ca", "ContributePermissionDenied"):
            valid_attempts = []
            accounts_with_non_followers = [
                acc_id for acc_id in existing_accounts if self._get_random_non_follower_of_account(acc_id)
            ]
            if accounts_with_non_followers:
                account_id = random.choice(accounts_with_non_followers)
                person_id = self._get_random_non_follower_of_account(account_id) # Should exist based on check
                article_id = self._get_random_non_existent_id("article")
                if person_id is not None and article_id is not None:
                     valid_attempts.append((person_id, account_id, article_id))

            if valid_attempts:
                 person_id, account_id, article_id = valid_attempts[0]
                 outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            valid_attempts = []
            accounts_with_followers = [
                acc_id for acc_id in existing_accounts if self._get_followers_of_account(acc_id)
            ]
            if accounts_with_followers:
                account_id = random.choice(accounts_with_followers)
                person_id = self._get_random_follower_of_account(account_id) # Should exist
                article_id = self._get_random_non_existent_id("article")
                if person_id is not None and article_id is not None:
                    valid_attempts.append((person_id, account_id, article_id))

            if valid_attempts:
                person_id, account_id, article_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or account_id is None or article_id is None: return None, None, None

        article_name = self._generate_random_name()
        params = {"person_id": person_id, "account_id": account_id, "article_id": article_id, "article_name": article_name}
        cmd_str = f"ca {person_id} {account_id} {article_id} {article_name}"
        return cmd_str, params, outcome

    def _generate_da(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
        existing_articles = self._get_existing_article_ids()
        person_id = None
        account_id = None
        article_id = None
        params = {}
        outcome = None

        if target_key == ("da", "PINF"):
            non_existent_person = self._get_random_non_existent_id("person")
            if non_existent_person is None: return None, None, None
            person_id = non_existent_person
            account_id = random.choice(existing_accounts) if existing_accounts else self._generate_random_id("account")
            article_id = random.choice(existing_articles) if existing_articles else self._generate_random_id("article")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("da", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                article_id = random.choice(existing_articles) if existing_articles else self._generate_random_id("article")
                if account_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("da", "AINF"):
            valid_attempts = []
            accounts_with_owners = [
                (acc_id, data['owner_id']) for acc_id, data in state['accounts'].items()
            ]
            random.shuffle(accounts_with_owners)
            for acc_id, owner_id in accounts_with_owners:
                 article_id = self._get_random_article_not_of_account(acc_id)
                 if article_id is not None:
                     person_id = owner_id
                     valid_attempts.append((person_id, acc_id, article_id))
                     break # Found one
            if valid_attempts:
                person_id, account_id, article_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("da", "DAPermissionDenied"):
            valid_attempts = []
            accounts_with_articles_and_non_owners = [
                acc_id for acc_id, data in state['accounts'].items()
                if self._get_articles_of_account(acc_id) and
                   [pid for pid in existing_persons if pid != data['owner_id']]
            ]
            if accounts_with_articles_and_non_owners:
                 account_id = random.choice(accounts_with_articles_and_non_owners)
                 acc_data = state['accounts'][account_id]
                 articles_of_account = self._get_articles_of_account(account_id)
                 non_owners = [pid for pid in existing_persons if pid != acc_data['owner_id']]
                 if articles_of_account and non_owners: # Check again
                      person_id = random.choice(non_owners)
                      article_id = random.choice(articles_of_account)
                      valid_attempts.append((person_id, account_id, article_id))

            if valid_attempts:
                person_id, account_id, article_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            valid_attempts = []
            accounts_with_articles_and_owner = [
                (acc_id, data['owner_id']) for acc_id, data in state['accounts'].items()
                if self._get_articles_of_account(acc_id)
            ]
            if accounts_with_articles_and_owner:
                account_id, owner_id = random.choice(accounts_with_articles_and_owner)
                articles_of_account = self._get_articles_of_account(account_id)
                if articles_of_account: # Check again
                    person_id = owner_id
                    article_id = random.choice(articles_of_account)
                    valid_attempts.append((person_id, account_id, article_id))

            if valid_attempts:
                person_id, account_id, article_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or account_id is None or article_id is None: return None, None, None

        params = {"person_id": person_id, "account_id": account_id, "article_id": article_id}
        cmd_str = f"da {person_id} {account_id} {article_id}"
        return cmd_str, params, outcome

    def _generate_foa(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
        person_id = None
        account_id = None
        params = {}
        outcome = None

        if target_key == ("foa", "PINF"):
            non_existent_person = self._get_random_non_existent_id("person")
            if non_existent_person is None: return None, None, None
            person_id = non_existent_person
            account_id = random.choice(existing_accounts) if existing_accounts else self._generate_random_id("account")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("foa", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                if account_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key == ("foa", "EPI_follower"):
            valid_attempts = []
            accounts_with_followers = [
                acc_id for acc_id in existing_accounts if self._get_followers_of_account(acc_id)
            ]
            if accounts_with_followers:
                account_id = random.choice(accounts_with_followers)
                followers = self._get_followers_of_account(account_id)
                if followers:
                    person_id = random.choice(followers)
                    valid_attempts.append((person_id, account_id))

            if valid_attempts:
                person_id, account_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            valid_attempts = []
            accounts_with_non_followers = [
                acc_id for acc_id in existing_accounts if self._get_random_non_follower_of_account(acc_id)
            ]
            if accounts_with_non_followers:
                 account_id = random.choice(accounts_with_non_followers)
                 person_id = self._get_random_non_follower_of_account(account_id)
                 if person_id is not None:
                      valid_attempts.append((person_id, account_id))

            if valid_attempts:
                 person_id, account_id = valid_attempts[0]
                 outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or account_id is None: return None, None, None

        params = {"person_id": person_id, "account_id": account_id}
        cmd_str = f"foa {person_id} {account_id}"
        return cmd_str, params, outcome

    def _generate_qsp(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None
        params = {}
        outcome = None

        if target_key and target_key[0] == "qsp" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) < 1: return None, None, None

            if target_key == ("qsp", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None: return None, None, None
            elif target_key == ("qsp", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("qsp", "PathNotFound"):
            if len(existing_ids) < 2: return None, None, None
            found_unreachable = False
            for _ in range(min(len(existing_ids), 10)): # Try a few times to find unreachable pair
                 start_node = random.choice(existing_ids)
                 reachable_set = self._bfs_reachable(start_node)
                 unreachable_candidates = [pid for pid in existing_ids if pid not in reachable_set]
                 if unreachable_candidates:
                     id1 = start_node
                     id2 = random.choice(unreachable_candidates)
                     found_unreachable = True
                     break
            if found_unreachable:
                 outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None # Assume network is connected or couldn't find pair

        elif target_key is None: # Try normal
            if not existing_ids: return None, None, None
            id1 = random.choice(existing_ids)
            reachable_set = self._bfs_reachable(id1)
            if reachable_set:
                id2 = random.choice(list(reachable_set)) # Might be id1 itself
                outcome = OUTCOME_NORMAL
            else: # Should only happen if id1 is invalid, which shouldn't occur here
                 return None, None, None
        else: return None, None, None

        if id1 is None or id2 is None: return None, None, None

        params = {"id1": id1, "id2": id2}
        cmd_str = f"qsp {id1} {id2}"
        return cmd_str, params, outcome

    def _generate_qbc(self, target_key=None):
        state = self.network_state
        existing_accounts = self._get_existing_account_ids()
        account_id = None
        params = {}
        outcome = None

        if target_key == ("qbc", "OAINF"):
            account_id = self._get_random_non_existent_id("account")
            if account_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None: # Try normal
            if existing_accounts:
                account_id = random.choice(existing_accounts)
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if account_id is None: return None, None, None

        params = {"account_id": account_id}
        cmd_str = f"qbc {account_id}"
        return cmd_str, params, outcome

    def _generate_qra(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        person_id = None
        params = {}
        outcome = None

        if target_key == ("qra", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None: # Try normal
            if existing_persons:
                person_id = random.choice(existing_persons)
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None: return None, None, None

        params = {"person_id": person_id}
        cmd_str = f"qra {person_id}"
        return cmd_str, params, outcome

    def _generate_qtvs(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        person_id = None
        tag_id = None
        params = {}
        outcome = None

        if target_key == ("qtvs", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            tag_id = self._generate_random_id("tag")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("qtvs", "TINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                tag_id = self._get_random_non_existent_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else: return None, None, None
        elif target_key is None: # Try normal
            persons_with_tags = [pid for pid in existing_persons if self._get_existing_tag_ids_for_person(pid)]
            if persons_with_tags:
                person_id = random.choice(persons_with_tags)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                 # QTVS specific: Ensure tag has members for meaningful result
                if not self._get_persons_in_tag(person_id, tag_id):
                    return None, None, None # Cannot generate normal if tag is empty
                outcome = OUTCOME_NORMAL
            else: return None, None, None
        else: return None, None, None

        if person_id is None or tag_id is None: return None, None, None

        params = {"person_id": person_id, "tag_id": tag_id}
        cmd_str = f"qtvs {person_id} {tag_id}"
        return cmd_str, params, outcome

    def _generate_qcs(self, target_key=None):
        if target_key is not None: return None, None, None
        cmd_str = "qcs"
        params = {}
        outcome = OUTCOME_NORMAL
        return cmd_str, params, outcome

    # --- State Update Functions (Ported from 评测机.txt as private methods) ---
    # These functions simulate the successful outcome of a *normal* command.
    # They modify self.network_state

    def _update_state_ap(self, params):
        state = self.network_state
        _id = params["id"]
        if _id not in state["persons"]:
            state["persons"][_id] = {"name": params["name"], "age": params["age"], "acquaintances": {}}
            state["person_tags"][_id] = set()
            state["received_articles"][_id] = deque()
            state["couple_sum_dirty"] = True

    def _update_state_ar(self, params):
        state = self.network_state
        id1 = params["id1"]
        id2 = params["id2"]
        value = params["value"]
        pair = (min(id1, id2), max(id1, id2))
        # Check conditions again just to be safe
        if id1 in state["persons"] and id2 in state["persons"] and id1 != id2 and pair not in state["relations"]:
            state["persons"][id1]["acquaintances"][id2] = value
            state["persons"][id2]["acquaintances"][id1] = value
            state["relations"][pair] = value
            # Update triple sum efficiently
            p1_acq = state["persons"][id1]["acquaintances"]
            p2_acq = state["persons"][id2]["acquaintances"]
            common_neighbors = set(p1_acq.keys()) & set(p2_acq.keys())
            # Ensure the common neighbors are actually linked to both p1 and p2 (redundant check if state is consistent)
            count = 0
            for neighbor_id in common_neighbors:
                 if neighbor_id != id1 and neighbor_id != id2: # Exclude the new pair itself
                     # Check if neighbor forms a triangle with the new edge
                     if (min(id1, neighbor_id), max(id1, neighbor_id)) in state["relations"] and \
                        (min(id2, neighbor_id), max(id2, neighbor_id)) in state["relations"]:
                          count += 1
            state["triple_sum"] += count
            state["couple_sum_dirty"] = True

    def _update_state_mr(self, params):
        state = self.network_state
        id1 = params["id1"]
        id2 = params["id2"]
        m_val = params["m_val"]
        pair = (min(id1, id2), max(id1, id2))
        if id1 in state["persons"] and id2 in state["persons"] and id1 != id2 and pair in state["relations"]:
            old_value = state["relations"][pair]
            new_value = old_value + m_val
            # Get common neighbors *before* modifying/deleting the relation
            p1_acq = state["persons"][id1]["acquaintances"]
            p2_acq = state["persons"][id2]["acquaintances"]
            common_neighbors_before = set(p1_acq.keys()) & set(p2_acq.keys())
            triangle_count_before = 0
            for neighbor_id in common_neighbors_before:
                if neighbor_id != id1 and neighbor_id != id2:
                    if (min(id1, neighbor_id), max(id1, neighbor_id)) in state["relations"] and \
                       (min(id2, neighbor_id), max(id2, neighbor_id)) in state["relations"]:
                        triangle_count_before +=1

            if new_value > 0:
                state["persons"][id1]["acquaintances"][id2] = new_value
                state["persons"][id2]["acquaintances"][id1] = new_value
                state["relations"][pair] = new_value
                state["couple_sum_dirty"] = True
            else: # value <= 0, relation is removed
                if id2 in state["persons"][id1]["acquaintances"]:
                     del state["persons"][id1]["acquaintances"][id2]
                if id1 in state["persons"][id2]["acquaintances"]:
                    del state["persons"][id2]["acquaintances"][id1]
                if pair in state["relations"]:
                    del state["relations"][pair]
                # Subtract the triangles that involved the removed edge
                state["triple_sum"] -= triangle_count_before
                state["couple_sum_dirty"] = True
                # Simulating tag removal side-effect is complex and skipped here, matching 评测机.txt

    def _update_state_at(self, params):
        state = self.network_state
        person_id = params["person_id"]
        tag_id = params["tag_id"]
        # Check conditions again
        if person_id in state["persons"] and tag_id not in state["person_tags"].get(person_id, set()):
            state["person_tags"].setdefault(person_id, set()).add(tag_id)
            state["tag_members"][(person_id, tag_id)] = {}

    def _update_state_dt(self, params):
        state = self.network_state
        person_id = params["person_id"]
        tag_id = params["tag_id"]
        # Check conditions again
        if person_id in state["persons"] and tag_id in state["person_tags"].get(person_id, set()):
            if person_id in state["person_tags"]: # Ensure key exists
                 state["person_tags"][person_id].discard(tag_id) # Use discard
                 if not state["person_tags"][person_id]: # Remove entry if set becomes empty
                      del state["person_tags"][person_id]
            tag_key = (person_id, tag_id)
            if tag_key in state["tag_members"]:
                del state["tag_members"][tag_key]

    def _update_state_att(self, params):
        state = self.network_state
        id1 = params["id1"] # member to add
        id2 = params["id2"] # owner
        tag_id = params["tag_id"]
        tag_key = (id2, tag_id)
        # Check normal conditions again
        if id1 in state["persons"] and id2 in state["persons"] and id1 != id2 and \
           (min(id1, id2), max(id1, id2)) in state["relations"] and \
           tag_id in state["person_tags"].get(id2, set()) and \
           id1 not in state["tag_members"].get(tag_key, {}) and \
           len(state["tag_members"].get(tag_key, {})) < TAG_PERSONS_LIMIT:
            p1_age = state["persons"][id1]["age"]
            state["tag_members"].setdefault(tag_key, {})[id1] = p1_age

    def _update_state_dft(self, params):
        state = self.network_state
        id1 = params["id1"] # member to delete
        id2 = params["id2"] # owner
        tag_id = params["tag_id"]
        tag_key = (id2, tag_id)
        # Check normal conditions again
        if id1 in state["persons"] and id2 in state["persons"] and \
           tag_id in state["person_tags"].get(id2, set()) and \
           id1 in state["tag_members"].get(tag_key, {}):
            if tag_key in state["tag_members"] and id1 in state["tag_members"][tag_key]:
                 del state["tag_members"][tag_key][id1]

    # --- New State Update Functions (Ported) ---
    def _update_state_coa(self, params):
        state = self.network_state
        person_id = params["person_id"]
        account_id = params["account_id"]
        account_name = params["account_name"]
        # Check conditions again
        if person_id in state["persons"] and account_id not in state["accounts"]:
            state["accounts"][account_id] = {
                "owner_id": person_id,
                "name": account_name,
                "followers": {person_id: 0},
                "articles": set(),
            }
            # No couple_sum_dirty flag change needed based on 评测机.txt logic

    def _update_state_doa(self, params):
        state = self.network_state
        person_id = params["person_id"]
        account_id = params["account_id"]
        # Check conditions again
        if person_id in state["persons"] and account_id in state["accounts"] and state["accounts"][account_id]["owner_id"] == person_id:
            if account_id in state["accounts"]:
                 del state["accounts"][account_id]
                 state["couple_sum_dirty"] = True

    def _update_state_ca(self, params):
        state = self.network_state
        person_id = params["person_id"]
        account_id = params["account_id"]
        article_id = params["article_id"]
        # Check conditions again
        if person_id in state["persons"] and account_id in state["accounts"] and \
           article_id not in state["articles_map"] and \
           person_id in state["accounts"][account_id]["followers"]:
            acc_data = state["accounts"][account_id]
            acc_data["articles"].add(article_id)
            state["articles_map"][article_id] = person_id
            acc_data["followers"][person_id] = acc_data["followers"].get(person_id, 0) + 1 # Ensure key exists
            for follower_id in acc_data["followers"].keys():
                if follower_id in state["received_articles"]:
                     state["received_articles"][follower_id].appendleft(article_id)
                     # Enforce limit
                     #if len(state["received_articles"][follower_id]) > ARTICLE_RECEIVED_LIMIT:
                     #    state["received_articles"][follower_id].pop() # Remove oldest if limit exceeded - JML doesn't specify this on CA, only QRA reads limited amount. Let's not pop here.
                #else: handle case where follower might not be in received_articles? Should not happen if state is consistent.

    def _update_state_da(self, params):
        state = self.network_state
        person_id = params["person_id"]
        account_id = params["account_id"]
        article_id = params["article_id"]
        # Check conditions again
        if person_id in state["persons"] and account_id in state["accounts"] and \
           article_id in state["accounts"][account_id].get("articles", set()) and \
           state["accounts"][account_id].get("owner_id") == person_id:
            acc_data = state["accounts"][account_id]
            contributor_id = state["articles_map"].get(article_id)
            acc_data.get("articles", set()).discard(article_id) # Use discard
            if article_id in state["articles_map"]:
                del state["articles_map"][article_id]
            if contributor_id is not None and contributor_id in acc_data.get("followers", {}):
                 acc_data["followers"][contributor_id] -= 1
                 if acc_data["followers"][contributor_id] <= 0: # Clean up if contribution becomes 0 or less
                     # Keep the follower entry, just set contributions to 0 as per implementation likely behavior?
                     # Or remove? Let's assume contribution just drops. It might not be possible to reach <0 anyway.
                     acc_data["followers"][contributor_id] = 0 # Reset to 0 if goes below

            # Remove article from followers' received lists
            for follower_id in acc_data.get("followers", {}).keys():
                 if follower_id in state["received_articles"]:
                     try:
                         # Need to remove *all* occurrences? JML is unclear. Let's remove first.
                         state["received_articles"][follower_id].remove(article_id)
                     except ValueError:
                         pass # Ignore if not found

    def _update_state_foa(self, params):
        state = self.network_state
        person_id = params["person_id"]
        account_id = params["account_id"]
        # Check conditions again
        if person_id in state["persons"] and account_id in state["accounts"] and \
           person_id not in state["accounts"][account_id].get("followers", {}):
            state["accounts"][account_id].setdefault("followers", {})[person_id] = 0
            state["couple_sum_dirty"] = True
            # Add existing articles to new follower's list (up to limit?)
            # 评测机 code did not simulate this distribution on follow, only on contribute.
            # Let's stick to that behavior. The new follower starts with an empty history essentially,
            # until new articles are contributed.

    _COMMAND_GENERATOR_METHODS = {
        "ap": _generate_ap, "ar": _generate_ar, "mr": _generate_mr,
        "at": _generate_at, "dt": _generate_dt, "att": _generate_att,
        "dft": _generate_dft, "qv": _generate_qv, "qci": _generate_qci,
        "qts": _generate_qts, "qtav": _generate_qtav, "qba": _generate_qba,
        "ln": _generate_ln,  # ln is special
        "coa": _generate_coa, "doa": _generate_doa, "ca": _generate_ca,
        "da": _generate_da, "foa": _generate_foa, "qsp": _generate_qsp,
        "qbc": _generate_qbc, "qra": _generate_qra, "qtvs": _generate_qtvs,
        "qcs": _generate_qcs,
    }

    # Helper dictionary mapping aliases to their update methods
    _ALIAS_TO_UPDATE_METHOD_MAP = {
        "ap": _update_state_ap, "ar": _update_state_ar, "mr": _update_state_mr,
        "at": _update_state_at, "dt": _update_state_dt, "att": _update_state_att,
        "dft": _update_state_dft,
        "coa": _update_state_coa, "doa": _update_state_doa, "ca": _update_state_ca,
        "da": _update_state_da, "foa": _update_state_foa,
        # Queries and ln do not have separate update methods here
    }

    # Now create the final map using the full command names as keys
    _STATE_UPDATE_METHODS = {
        # Lookup the full name using the alias from the helper map's keys
        INSTRUCTION_MAP.get(alias): method
        # Iterate through the helper map
        for alias, method in _ALIAS_TO_UPDATE_METHOD_MAP.items()
        # Add a check just in case INSTRUCTION_MAP is incomplete, though it shouldn't be
        if INSTRUCTION_MAP.get(alias) is not None
    }


    # --- Phase Management (Unchanged from Original DataGenerator) ---
    def _update_phase(self):
        total_target = self.target_instructions
        instr_count = self.instructions_generated
        # Adjust phase boundaries if needed
        build_end = total_target * 0.30
        mix_end = total_target * 0.75
        if self.current_phase == GenPhase.INITIAL_LOAD:
            self.current_phase = GenPhase.BUILD_NETWORK
            self.phase_instruction_count = 0
        elif self.current_phase == GenPhase.BUILD_NETWORK and instr_count >= build_end:
            self.current_phase = GenPhase.RANDOM_MIX
            self.phase_instruction_count = 0
        elif self.current_phase == GenPhase.RANDOM_MIX and instr_count >= mix_end:
            self.current_phase = GenPhase.STRESS_COMPLEX
            self.phase_instruction_count = 0
        self.phase_instruction_count += 1

    # --- load_network (Adapted to use ported _generate_ln) ---
    def generate_load_network(self):
        """Generates the initial ln command and updates state."""
        # Only generate ln if it's the very first instruction
        if self.instructions_generated > 0:
            return 0

        # Call the ported ln generator
        result = self._generate_ln(target_key=None)

        if result and result[0] is not None:
            cmd_str, _, _ = result
            # Add the multi-line output of ln
            # Split the output string correctly
            lines = cmd_str.strip().split('\n')
            self.generated_lines.extend(lines)
            # State update is handled *within* _generate_ln
            print(f"Generated load_network (first line: {lines[0]}). State updated.")
            return 1 # Counts as one logical instruction
        else:
            print("Error: Failed to generate initial load_network command.", file=sys.stderr)
            # Handle this failure case - maybe generate a default small network?
            # Fallback: Generate a tiny network manually if _generate_ln fails
            self._initialize_state() # Reset state again
            n = 2
            ids = [1, 2]
            names = ["p1", "p2"]
            ages = [20, 30]
            self.generated_lines.extend([
                f"ln {n}",
                " ".join(map(str, ids)),
                " ".join(names),
                " ".join(map(str, ages))
                 # No relations line needed for n=2 if no relations added below
            ])
            # Manually update state for this fallback
            self.network_state["persons"][1] = {"name": "p1", "age": 20, "acquaintances": {}}
            self.network_state["person_tags"][1] = set()
            self.network_state["received_articles"][1] = deque()
            self.network_state["persons"][2] = {"name": "p2", "age": 30, "acquaintances": {}}
            self.network_state["person_tags"][2] = set()
            self.network_state["received_articles"][2] = deque()
            print("Warning: _generate_ln failed, generated minimal fallback ln.")
            return 1


    # --- _generate_arguments (NEW - Integrates ported generators and updaters) ---
    def _generate_arguments(self, cmd_alias, force_valid=False, force_exception_name=None):
        """
        Generates arguments using the ported methods.
        Handles state updates for successful normal commands.
        Updates guarantee tracking.
        Returns the command string or None.
        """
        target_key = None
        attempted_exception = False

        # 1. Determine target_key
        if force_valid:
            target_key = None
        elif force_exception_name:
            attempted_exception = True
            # Find a matching target_key for this exception name and command
            possible_keys = [k for k in EXCEPTION_NAME_TO_TARGET_KEY.get(force_exception_name, []) if k[0] == cmd_alias]
            if possible_keys:
                target_key = random.choice(possible_keys)
                self.exceptions_attempted.add(target_key) # Mark attempt
            else:
                 # This exception isn't valid for this command according to map
                 # print(f"Debug: Invalid exception {force_exception_name} forced for {cmd_alias}", file=sys.stderr)
                 return None # Cannot fulfill request
        else:
            # Random Generation: Mix of Valid and Error
            error_attempt_prob = 0.4 # Probability to try generating an exception
            if random.random() < error_attempt_prob:
                possible_exceptions = list(EXCEPTION_MAP.get(cmd_alias, set()))
                if possible_exceptions:
                    target_exc_name = random.choice(possible_exceptions)
                    possible_keys = [k for k in EXCEPTION_NAME_TO_TARGET_KEY.get(target_exc_name, []) if k[0] == cmd_alias]
                    if possible_keys:
                         target_key = random.choice(possible_keys)
                         attempted_exception = True
                         # Don't mark attempt here, only if forced or successful? Let's mark here for coverage.
                         self.exceptions_attempted.add(target_key)

            if target_key is None: # If no exception was chosen or possible
                 target_key = None # Aim for normal

        # 2. Get the appropriate generator method
        generator_method = self._COMMAND_GENERATOR_METHODS.get(cmd_alias)
        if not generator_method:
            print(f"Error: No generator method found for command alias '{cmd_alias}'", file=sys.stderr)
            return None

        # 3. Call the generator
        result = generator_method(self, target_key=target_key) # Pass self

        # 4. Process the result
        if result is None or result[0] is None:
             # Generation failed for this specific target/state
             # If we were trying an exception, the attempt is already marked.
             return None

        cmd_str, params, outcome = result

        # 5. Update state if it was a successful NORMAL operation
        if outcome == OUTCOME_NORMAL:
            self.commands_successfully_generated.add(cmd_alias) # Mark success
            full_command_name = INSTRUCTION_MAP.get(cmd_alias)
            update_method = self._STATE_UPDATE_METHODS.get(full_command_name)
            if update_method:
                try:
                    update_method(self, params) # Pass self
                except Exception as e:
                    print(f"CRITICAL ERROR during state update for {cmd_alias} (normal): {e}", file=sys.stderr)
                    print(f"Params: {params}", file=sys.stderr)
                    # Decide whether to continue or exit? Let's continue but log error.
            # Else: No state update needed (e.g., query commands)
        elif outcome != OUTCOME_NORMAL:
            # An exception was successfully generated
            # We already marked the attempt if it was forced or randomly chosen.
            # Ensure the target_key used corresponds to the outcome exception name.
            expected_exc_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
            if outcome != expected_exc_name:
                 print(f"Warning: Generator for {target_key} produced outcome '{outcome}' but map expected '{expected_exc_name}'", file=sys.stderr)
            # Mark the attempt if it wasn't marked before (e.g., if normal was intended but exception occurred - shouldn't happen)
            if target_key: # If we were targeting an exception
                self.exceptions_attempted.add(target_key)
            # else: # If normal was intended but exception resulted - log error
            #    print(f"ERROR: Generator for {cmd_alias} (target=None) resulted in exception outcome '{outcome}'", file=sys.stderr)


        return cmd_str


    # --- generate_instruction (Selects command, handles guarantee - REVISED FOR NEW STRUCTURE) ---
    def generate_instruction(self):
        """Selects a command alias and calls _generate_arguments."""
        selected_cmd_alias = None
        force_valid = False
        force_exception_name = None
        coverage_attempt_prob = 0.15 # Check ~15% of the time

        # --- Guarantee Logic ---
        pending_success_cmds = list(COMMANDS - self.commands_successfully_generated) # Use aliases
        pending_exception_keys = list(self.all_exceptions_to_attempt - self.exceptions_attempted) # Use target_keys
        action_taken = False

        # Prioritize guarantees if pending and chance allows
        if (pending_success_cmds or pending_exception_keys) and random.random() < coverage_attempt_prob:
            prioritize_success_prob = 0.6
            if pending_success_cmds and (not pending_exception_keys or random.random() < prioritize_success_prob):
                selected_cmd_alias = random.choice(pending_success_cmds)
                force_valid = True
                action_taken = True
                # print(f"DEBUG: Prioritize VALID {selected_cmd_alias}")
            elif pending_exception_keys:
                target_key = random.choice(pending_exception_keys)
                selected_cmd_alias, _ = target_key
                force_exception_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
                if force_exception_name: # Ensure map lookup worked
                    action_taken = True
                    # print(f"DEBUG: Prioritize EXCEPTION {target_key} ({force_exception_name})")
                else:
                    print(f"Warning: Could not find exception name for target_key {target_key} in map.", file=sys.stderr)
                    self.exceptions_attempted.add(target_key) # Mark as attempted even if name lookup failed


        # --- Weighted Random Selection (Fallback) ---
        if not action_taken:
            weights = defaultdict(float)
            current_phase = self.current_phase
            # Define weights based on phase (example weights - can be tuned)
            # Using weights similar to the original '评测机.txt' structure
            COMMAND_WEIGHTS = { # From 评测机.txt, adjusted slightly maybe
                "ap": 10, "ar": 10, "mr": 10, "at": 8, "dt": 5, "att": 8,
                "dft": 5, "qv": 8, "qci": 8, "qts": 3, "qtav": 10, "qtvs": 10,
                "qba": 8, "qcs": 8, "coa": 8, "doa": 5, "ca": 10, "da": 7,
                "foa": 8, "qsp": 12, "qbc": 6, "qra": 6,
            }
            weights = defaultdict(float, COMMAND_WEIGHTS)


            # State Pruning (Essential - Querying self.network_state)
            runnable_cmds = list(COMMANDS) # Start with all possible non-ln commands
            temp_weights = weights.copy()
            state = self.network_state

            if not state["persons"]:
                 # If no persons, only 'ap' makes sense.
                 runnable_cmds = ['ap']
                 cmd_weights = [1.0]
            else:
                num_persons = len(state["persons"])
                num_relations = len(state["relations"])
                num_tags_owned = sum(1 for tags in state["person_tags"].values() if tags)
                num_tag_members = sum(len(m) for m in state["tag_members"].values())
                num_accounts = len(state["accounts"])
                num_articles = len(state["articles_map"])
                num_followers = sum(len(d.get("followers",{})) for d in state["accounts"].values())

                if num_persons < 2:
                    for cmd in ['ar', 'mr', 'qv', 'qci', 'att', 'dft', 'qsp', 'qtvs', 'qtav', 'qcs', 'qba']: # Commands needing >= 2 people or relations derived from them
                        temp_weights[cmd] = 0
                if num_relations == 0:
                    for cmd in ['mr', 'qv', 'att', 'qba']: # Need existing relations or acquaintances
                         temp_weights[cmd] = 0
                if num_tags_owned == 0:
                     for cmd in ['dt', 'qtvs', 'qtav', 'att', 'dft']: # Need owned tags
                         temp_weights[cmd] = 0
                if num_tag_members == 0:
                     for cmd in ['dft', 'qtav', 'qtvs']: # Need members in tags for normal operation/meaningful query
                         temp_weights[cmd] = 0
                if num_accounts == 0:
                    for cmd in ['doa','ca','da','foa','qbc', 'qra']: # Need accounts
                        temp_weights[cmd] = 0
                if num_articles == 0:
                    # 'da' needs articles IN accounts, ca needs possibility to add
                    if not any(acc.get("articles") for acc in state["accounts"].values()):
                         temp_weights['da'] = 0
                if num_followers == 0 :
                     # 'ca' needs followers (owner is initially one)
                     if not any(acc.get("followers") for acc in state["accounts"].values()):
                          temp_weights['ca'] = 0


                # Final runnable list based on non-zero weights after pruning
                runnable_cmds_final = []
                cmd_weights_final = []
                # Use COMMANDS which is the set of aliases
                for cmd_alias in COMMANDS:
                    w = temp_weights.get(cmd_alias, 0)
                    if w > 0 :
                        runnable_cmds_final.append(cmd_alias)
                        cmd_weights_final.append(w)

                if not runnable_cmds_final:
                    selected_cmd_alias = 'ap' # Ultimate fallback if pruning removed everything
                    cmd_weights_final = [1.0]
                    runnable_cmds_final = ['ap']

                # Check if weights sum to zero before choices
                if sum(cmd_weights_final) > 0:
                    selected_cmd_alias = random.choices(runnable_cmds_final, cmd_weights_final, k=1)[0]
                elif runnable_cmds_final: # If weights are all zero but list not empty
                     selected_cmd_alias = random.choice(runnable_cmds_final) # Equal chance
                else:
                     selected_cmd_alias = 'ap' # Absolute fallback


        # --- Generate and Return ---
        if selected_cmd_alias:
            return self._generate_arguments(selected_cmd_alias, force_valid=force_valid, force_exception_name=force_exception_name)
        else:
            print("Warning: Failed to select any command alias in generate_instruction.", file=sys.stderr)
            # Attempt fallback generation?
            return self._generate_arguments('ap', force_valid=True) # Try adding a person


    # --- generate method (Main loop - REVISED with better final guarantee) ---
    def generate(self):
        self._initialize_state()
        # Generate ln first (adds lines directly, updates state)
        self.instructions_generated += self.generate_load_network()
        self._update_phase() # Update phase after ln attempt

        attempts = 0
        max_attempts_factor = 25 # Allow more attempts
        max_total_attempts = self.target_instructions * max_attempts_factor
        stuck_counter = 0
        max_stuck_count = max(500, self.target_instructions // 4)

        while self.instructions_generated < self.target_instructions and attempts < max_total_attempts:
            instr_str = self.generate_instruction()
            if instr_str:
                self.generated_lines.append(instr_str)
                self.instructions_generated += 1
                self._update_phase()
                stuck_counter = 0
            else:
                stuck_counter += 1

            attempts += 1
            if stuck_counter >= max_stuck_count:
                print(f"Warning: Potential stall ({stuck_counter} attempts without generation), breaking main loop.", file=sys.stderr)
                break

        # --- Final Guarantee Attempt ---
        print("\n--- Starting Final Guarantee Phase ---")
        final_loop_start_time = time.time()

        # Try ensuring each command has SUCCEEDED once
        missing_success_cmds = list(COMMANDS - self.commands_successfully_generated) # Aliases
        final_success_attempts = 0
        max_final_success_attempts = len(missing_success_cmds) * 25 # More attempts

        while missing_success_cmds and final_success_attempts < max_final_success_attempts and len(self.generated_lines) < self.max_instr_limit:
            final_success_attempts += 1
            cmd_to_add = missing_success_cmds[0]
            # print(f"Final Guarantee: Trying VALID {cmd_to_add}")
            instr_str = self._generate_arguments(cmd_to_add, force_valid=True)
            if instr_str:
                self.generated_lines.append(instr_str)
                # Check if it was *actually* added to successful set now
                if cmd_to_add in self.commands_successfully_generated:
                     # print(f"Final Guarantee: Success for {cmd_to_add}")
                     missing_success_cmds.pop(0)
                else:
                     # It generated but maybe didn't update the set? Or failed silently? Rotate.
                     # print(f"Final Guarantee: Generated {cmd_to_add} but not marked success? Rotating.")
                     missing_success_cmds.append(missing_success_cmds.pop(0))
            else:
                 # print(f"Final Guarantee: Failed VALID {cmd_to_add}. Rotating.")
                 missing_success_cmds.append(missing_success_cmds.pop(0)) # Rotate on failure


        # Then try ensuring each exception has been ATTEMPTED once
        missing_exception_keys = list(self.all_exceptions_to_attempt - self.exceptions_attempted) # Target keys
        final_exception_attempts = 0
        max_final_exception_attempts = len(missing_exception_keys) * 15 # More attempts

        while missing_exception_keys and final_exception_attempts < max_final_exception_attempts and len(self.generated_lines) < self.max_instr_limit:
             final_exception_attempts += 1
             target_key = missing_exception_keys[0]
             cmd_alias, _ = target_key
             exc_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
             # print(f"Final Guarantee: Trying EXCEPTION {target_key} ({exc_name})")

             if exc_name:
                  instr_str = self._generate_arguments(cmd_alias, force_exception_name=exc_name)
                  # We mark attempt inside _generate_arguments when forced, so just pop if found
                  if target_key in self.exceptions_attempted:
                      # print(f"Final Guarantee: Attempt recorded for {target_key}")
                      missing_exception_keys.pop(0)
                      if instr_str:
                          self.generated_lines.append(instr_str)
                  else:
                      # print(f"Final Guarantee: Failed EXCEPTION {target_key}. Rotating.")
                      missing_exception_keys.append(missing_exception_keys.pop(0)) # Rotate if attempt failed
             else:
                  # print(f"Final Guarantee: Cannot find exception name for {target_key}. Skipping and marking attempted.")
                  self.exceptions_attempted.add(target_key) # Mark attempt even if name lookup failed
                  missing_exception_keys.pop(0)


        print(f"--- Finished Final Guarantee Phase (took {time.time() - final_loop_start_time:.2f}s) ---")

        # Final Report
        if attempts >= max_total_attempts:
            print(f"Warning: Generator hit max attempts ({max_total_attempts}) during main phase.")

        missing_success_final = list(COMMANDS - self.commands_successfully_generated)
        missing_exceptions_final = list(self.all_exceptions_to_attempt - self.exceptions_attempted)

        if missing_success_final:
            print(f"Warning: Could not guarantee successful generation for: {sorted(list(missing_success_final))}")
        if missing_exceptions_final:
            print(f"Warning: Could not guarantee exception attempt for: {sorted([f'{tk[0]}-{GENERATOR_TARGET_OUTCOME_MAP.get(tk)}' for tk in missing_exceptions_final])}")

        print(f"Generator finished: generated {len(self.generated_lines)} lines (target logical: {self.target_instructions}).")
        print(f"Successfully generated commands guaranteed: {len(self.commands_successfully_generated)}/{len(COMMANDS)}")
        print(f"Attempted exceptions guaranteed: {len(self.exceptions_attempted)}/{len(self.all_exceptions_to_attempt)}")

        # Make sure all lines are strings
        final_lines = [str(line) for line in self.generated_lines if line is not None]
        return final_lines


# --- Main execution (Unchanged from Original DataGenerator) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Comprehensive HW10 Test Data (Integrated Logic).")
    parser.add_argument("-m", "--mode", choices=['P', 'M'], default='P', help="Test mode (P=Public, M=Mutual)")
    parser.add_argument("-n", "--num_instructions", type=int, default=3000, help="Target number of LOGICAL instructions")
    parser.add_argument("-o", "--output", type=str, default="generated_hw10_integrated_data.txt", help="Output file name")
    args = parser.parse_args()

    start_time = time.time()
    generator = DataGenerator(mode=args.mode, num_logical_instructions=args.num_instructions)
    generated_instruction_lines = generator.generate()
    end_time = time.time()
    print(f"\nGeneration took {end_time - start_time:.2f} seconds.")

    try:
        with open(args.output, "w", encoding="utf-8") as f:
            # Handle potential multi-line ln output correctly
            current_line_count = 0
            for line_content in generated_instruction_lines:
                # Check if line_content itself is multi-line (from ln)
                lines_to_write = str(line_content).split('\n')
                for single_line in lines_to_write:
                    if current_line_count < generator.max_instr_limit:
                         f.write(single_line + "\n")
                         current_line_count += 1
                    else:
                        print(f"Warning: Truncated output at {generator.max_instr_limit} lines.", file=sys.stderr)
                        break
                if current_line_count >= generator.max_instr_limit:
                    break

        print(f"Successfully wrote {current_line_count} lines to {args.output}")
    except IOError as e:
        print(f"Error writing to output file {args.output}: {e}")