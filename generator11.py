import random
import sys
import string
import math
from collections import defaultdict, deque
import os
import argparse
import enum
import time

# --- ALIAS MAP (Updated for HW11) ---
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
    # HW11 New Commands
    "add_message": "am",  # Ordinary message
    "send_message": "sm",
    "query_social_value": "qsv",
    "query_received_messages": "qrm",
    "add_red_envelope_message": "arem",
    "add_forward_message": "afm",
    "add_emoji_message": "aem",
    "store_emoji_id": "sei",
    "query_popularity": "qp",
    "delete_cold_emoji": "dce",
    "query_money": "qm",
}
INSTRUCTION_MAP = {v: k for k, v in ALIAS_MAP.items()}

# --- Parameter Constraints (Updated/Reviewed for HW11) ---
AGE_RANGE = (1, 200)
VALUE_RANGE = (1, 200)
MVAL_RANGE = (-200, 200)
NAME_LENGTH_RANGE = (1, 10)
N_RANGE = (1, 200)  # Max N for ln is platform dependent, keep this as general target
TAG_PERSONS_LIMIT = 999
ARTICLE_RECEIVED_LIMIT = 5  # For Person.queryReceivedArticles()
MESSAGES_RECEIVED_LIMIT = 5  # For Person.getReceivedMessages() query part

ID_POOL_RANGE = (-150, 150)
TAG_ID_POOL_RANGE = (-150, 150)
ACCOUNT_ID_POOL_RANGE = (-150, 150)
ARTICLE_ID_POOL_RANGE = (-150, 150)
MESSAGE_ID_POOL_RANGE = (-2000, 2000)  # New for message IDs
EMOJI_ID_POOL_RANGE = (-1000, 1000)  # As per requirement
SOCIAL_VALUE_RANGE = (-1000, 1000)  # For ordinary message social value
MONEY_RANGE_PERSON = (-5000, 5000)  # Person's money can be negative
MONEY_RANGE_RED_ENVELOPE = (1, 200)  # Red envelope money should be positive
LIMIT_RANGE_DCE = (-2000, 2000)  # For delete_cold_emoji limit

# --- Exception/Outcome Keys (Updated for HW11) ---
OUTCOME_NORMAL = "normal"
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
    ("att", "EPI_id1_eq_id2"): "EqualPersonIdException",  # This is for personId1 == personId2 in instruction
    ("att", "RNF"): "RelationNotFoundException",
    ("att", "TINF"): "TagIdNotFoundException",
    ("att", "EPI_in_tag"): "EqualPersonIdException",  # This is for personId1 already in tag
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
    ("doa", "DAPermissionDenied_DOA"): "DeleteOfficialAccountPermissionDeniedException",  # Specific name
    ("ca", "PINF"): "PersonIdNotFoundException",
    ("ca", "OAINF"): "OfficialAccountIdNotFoundException",
    ("ca", "EAI"): "EqualArticleIdException",
    ("ca", "ContributePermissionDenied"): "ContributePermissionDeniedException",
    ("da", "PINF"): "PersonIdNotFoundException",
    ("da", "OAINF"): "OfficialAccountIdNotFoundException",
    ("da", "AINF"): "ArticleIdNotFoundException",
    ("da", "DAPermissionDenied_DA"): "DeleteArticlePermissionDeniedException",  # Specific name
    ("foa", "PINF"): "PersonIdNotFoundException",
    ("foa", "OAINF"): "OfficialAccountIdNotFoundException",
    ("foa", "EPI_follower"): "EqualPersonIdException",  # person already follower
    ("qsp", "PINF_id1"): "PersonIdNotFoundException",
    ("qsp", "PINF_id2"): "PersonIdNotFoundException",
    ("qsp", "PathNotFound"): "PathNotFoundException",
    ("qbc", "OAINF"): "OfficialAccountIdNotFoundException",
    ("qra", "PINF"): "PersonIdNotFoundException",
    ("qtvs", "PINF"): "PersonIdNotFoundException",
    ("qtvs", "TINF"): "TagIdNotFoundException",
    # HW11 New Exception Mappings
    ("am", "EMIE"): "EqualMessageIdException",
    ("am", "EPI_msg"): "EqualPersonIdException",  # type 0, p1 == p2
    # Note: For am, aem, arem, afm, the PINF/TINF for p1/p2/tag_id are handled by Runner usually.
    # Network's addMessage has EMINFE, AINFE.
    ("aem", "EMIE"): "EqualMessageIdException",
    ("aem", "EMINFE"): "EmojiIdNotFoundException",  # Emoji ID not stored in network
    ("aem", "EPI_msg"): "EqualPersonIdException",
    ("arem", "EMIE"): "EqualMessageIdException",
    ("arem", "EPI_msg"): "EqualPersonIdException",
    ("afm", "EMIE"): "EqualMessageIdException",
    ("afm", "AINFE_net"): "ArticleIdNotFoundException",  # Article ID not in network.articles
    ("afm", "AINFE_person"): "ArticleIdNotFoundException",  # Article ID not in person1's received list
    ("afm", "EPI_msg"): "EqualPersonIdException",
    ("sm", "MINFE"): "MessageIdNotFoundException",
    ("sm", "RNF_sm"): "RelationNotFoundException",  # type 0, p1 not linked to p2
    ("sm", "TINF_sm"): "TagIdNotFoundException",  # type 1, p1 does not have tag
    ("qsv", "PINF"): "PersonIdNotFoundException",
    ("qrm", "PINF"): "PersonIdNotFoundException",
    ("sei", "EEIE"): "EqualEmojiIdException",  # Emoji ID already stored
    ("qp", "EMINFE_qp"): "EmojiIdNotFoundException",  # query popularity for non-existent emoji id
    ("qm", "PINF"): "PersonIdNotFoundException",
}
ALL_TARGET_KEYS = list(GENERATOR_TARGET_OUTCOME_MAP.keys())
EXCEPTION_NAME_TO_TARGET_KEY = defaultdict(list)
for key, name in GENERATOR_TARGET_OUTCOME_MAP.items():
    EXCEPTION_NAME_TO_TARGET_KEY[name].append(key)

# --- Constants ---
PUBLIC_MAX_INSTRUCTIONS = 10000
PUBLIC_MAX_N_LOAD = 300
MUTUAL_MAX_INSTRUCTIONS = 3000
MUTUAL_MAX_N_LOAD = 100

COMMANDS = set(ALIAS_MAP.values()) - {'ln'}
LOAD_CMDS = {"ln"}
COMMANDS_WITH_ARGS = COMMANDS

EXCEPTION_MAP = defaultdict(set)
for (cmd_alias, _), exc_name in GENERATOR_TARGET_OUTCOME_MAP.items():
    EXCEPTION_MAP[cmd_alias].add(exc_name)


class GenPhase(enum.Enum):
    INITIAL_LOAD = 0
    BUILD_NETWORK = 1
    RANDOM_MIX = 2
    STRESS_COMPLEX = 3


# --- Generator Class ---
class DataGenerator:
    def __init__(self, mode='P', num_logical_instructions=100):
        self.mode = mode.upper()
        self.target_instructions = num_logical_instructions
        if self.mode == 'P':
            self.max_instr_limit = PUBLIC_MAX_INSTRUCTIONS
            self.max_n_load_limit = PUBLIC_MAX_N_LOAD
        else:
            self.max_instr_limit = MUTUAL_MAX_INSTRUCTIONS
            self.max_n_load_limit = MUTUAL_MAX_N_LOAD
        self.target_instructions = min(self.target_instructions, self.max_instr_limit)
        self._initialize_state()

    def _initialize_state(self):
        self.network_state = {
            "persons": {},
            # id -> {name, age, acquaintances: {id:val}, tags: {tag_id}, money: M, socialValue: SV, messages_received_obj: deque(), articles_received_ids: deque()}
            "person_tags": {},  # (person_id, tag_id) -> {member_id: age} # Stores actual members of a tag
            "relations": {},  # (min_id, max_id) -> value
            "accounts": {},
            # account_id -> {owner_id, name, followers: {person_id: contribution_count}, articles: {article_id}}
            "articles_map": {},  # article_id -> contributor_person_id (Original contributor)
            "triple_sum": 0,
            "couple_sum_dirty": True,
            # HW11 additions to network_state
            "messages_map": {},  # message_id -> message_object (see _generate_message_object_structure)
            "emoji_id_list": [],  # List of stored emoji IDs
            "emoji_heat_list": [],  # Corresponding heat for emoji_id_list
            "next_message_id_counter": 0,  # Simple counter for unique message IDs
            "all_message_ids_ever_used": set()  # <--- NEW: Track all used message IDs
        }
        self.instructions_generated = 0
        self.generated_lines = []
        self.current_phase = GenPhase.INITIAL_LOAD
        self.phase_instruction_count = 0
        self.commands_successfully_generated = set()
        self.exceptions_attempted = set()
        self.all_exceptions_to_attempt = set(ALL_TARGET_KEYS)

    def _get_next_message_id(self):
        # A simple way to get a unique message ID. Can be made more robust.
        # For now, just use a counter that's unlikely to collide with MESSAGE_ID_POOL_RANGE if that's used for *input*
        # Or, ensure MESSAGE_ID_POOL_RANGE is only for input, and internal generation uses this.
        # Let's assume generated IDs from pool and this counter don't need to be exclusive.
        # The JML for addMessage is !containsMessage(id), so the *input* id matters.
        # This helper is for when we internally *create* a message not from direct input.
        # For direct input like `am ID ...`, the ID comes from the instruction.
        # This is more for internal tracking if needed, but message IDs are usually given.
        # Let's reconsider if this counter is truly needed.
        # Message IDs are *given* in the input commands. So `next_message_id_counter` isn't used for generating the ID *for the command string*.
        # It might be useful if we were to, for example, *create* a message object internally for some complex simulation,
        # but for generating commands, the ID is an input parameter.
        # I'll keep it for now in case it's useful for some complex state logic later, but it's not directly used for command string generation.
        self.network_state["next_message_id_counter"] += 1
        return self.network_state["next_message_id_counter"] + max(MESSAGE_ID_POOL_RANGE)  # Offset to avoid pool

    # --- State Query Helper Functions (Updated/Reviewed for HW11) ---
    def _get_existing_person_ids(self):
        return list(self.network_state["persons"].keys())

    def _get_existing_account_ids(self):
        return list(self.network_state["accounts"].keys())

    def _get_existing_article_ids(self):
        return list(self.network_state["articles_map"].keys())

    def _get_existing_message_ids(self):
        return list(self.network_state["messages_map"].keys())

    def _get_existing_stored_emoji_ids(self):
        return list(self.network_state["emoji_id_list"])

    def _get_existing_relation_pairs(self):
        return list(self.network_state["relations"].keys())

    def _get_existing_tag_ids_for_person(self, person_id):
        person_data = self.network_state["persons"].get(person_id)
        return list(person_data["tags"]) if person_data and "tags" in person_data else []

    def _get_persons_in_tag(self, owner_id, tag_id):  # owner_id is person who owns the tag
        return list(self.network_state["person_tags"].get((owner_id, tag_id), {}).keys())

    def _get_accounts_owned_by_person(self, person_id):
        return [acc_id for acc_id, acc_data in self.network_state["accounts"].items() if
                acc_data["owner_id"] == person_id]

    def _get_followers_of_account(self, account_id):
        acc_data = self.network_state["accounts"].get(account_id)
        return list(acc_data["followers"].keys()) if acc_data and "followers" in acc_data else []

    def _get_articles_of_account(self, account_id):  # Articles *created by* this account
        acc_data = self.network_state["accounts"].get(account_id)
        return list(acc_data["articles"]) if acc_data and "articles" in acc_data else []

    def _get_articles_received_by_person(self, person_id):
        person_data = self.network_state["persons"].get(person_id)
        return list(
            person_data["articles_received_ids"]) if person_data and "articles_received_ids" in person_data else []

    def _get_messages_received_by_person_obj(self, person_id):
        person_data = self.network_state["persons"].get(person_id)
        return list(
            person_data["messages_received_obj"]) if person_data and "messages_received_obj" in person_data else []

    def _bfs_reachable(self, start_id):
        state = self.network_state
        if start_id not in state["persons"]:
            return set()
        reachable = set()
        queue = deque([start_id])
        visited = {start_id}
        reachable.add(start_id)
        while queue:
            current_id = queue.popleft()
            current_person_data = state["persons"].get(current_id)
            if current_person_data and "acquaintances" in current_person_data:
                for acquaintance_id in current_person_data["acquaintances"].keys():
                    if acquaintance_id not in visited:
                        visited.add(acquaintance_id)
                        reachable.add(acquaintance_id)
                        queue.append(acquaintance_id)
        return reachable

    # --- Helper Functions for Parameter Generation (Updated/Reviewed for HW11) ---
    def _generate_random_id(self, id_type="person", used_ids=None, pool_range_override=None):
        # Determine pool_range
        if pool_range_override:
            pool_range = pool_range_override
        elif id_type == "person":
            pool_range = ID_POOL_RANGE
        elif id_type == "account":
            pool_range = ACCOUNT_ID_POOL_RANGE
        elif id_type == "article":
            pool_range = ARTICLE_ID_POOL_RANGE
        elif id_type == "tag":
            pool_range = TAG_ID_POOL_RANGE
        elif id_type == "message":
            pool_range = MESSAGE_ID_POOL_RANGE
        elif id_type == "emoji":
            pool_range = EMOJI_ID_POOL_RANGE
        else:
            raise ValueError(f"Unknown ID type: {id_type}")

        id_pool_list = list(range(pool_range[0], pool_range[1] + 1))
        if not id_pool_list:  # Empty pool
            if used_ids is None: return pool_range[0]  # Default if pool is just one invalid number
            return None  # Cannot generate

        if used_ids is not None:
            available_ids = list(set(id_pool_list) - set(used_ids))
            if available_ids:
                return random.choice(available_ids)
            else:  # No available IDs from the pool that are not used
                return None  # Cannot generate a new one from this pool
        return random.choice(id_pool_list)  # Pick any from pool if no used_ids restriction

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
        elif id_type == "message":
            existing_ids = self._get_existing_message_ids()
            pool_range = MESSAGE_ID_POOL_RANGE
        elif id_type == "emoji":  # For "non-existent stored emoji id"
            existing_ids = self._get_existing_stored_emoji_ids()
            pool_range = EMOJI_ID_POOL_RANGE
        elif id_type == "tag":  # For non-existent tag for a *specific person*
            return None  # This helper is not suitable for "non-existent tag for person"
        else:
            raise ValueError(f"Unknown ID type: {id_type}")

        all_possible_ids_in_pool = set(range(pool_range[0], pool_range[1] + 1))
        if not all_possible_ids_in_pool: return None

        existing_ids_set = set(existing_ids)
        available_ids = list(all_possible_ids_in_pool - existing_ids_set)
        return random.choice(available_ids) if available_ids else None

    def _get_random_non_existent_tag_id_for_person(self, person_id):
        # Get tags *owned* by this person
        person_data = self.network_state["persons"].get(person_id)
        if not person_data: return None  # Person doesn't exist
        existing_tag_ids_owned = list(person_data.get("tags", set()))

        tag_pool = set(range(TAG_ID_POOL_RANGE[0], TAG_ID_POOL_RANGE[1] + 1))
        if not tag_pool: return None

        available_tag_ids = list(tag_pool - set(existing_tag_ids_owned))
        return random.choice(available_tag_ids) if available_tag_ids else None

    def _generate_random_name(self, length_range=NAME_LENGTH_RANGE):
        length = random.randint(length_range[0], max(1, length_range[1]))
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def _generate_random_age(self, age_range=AGE_RANGE):
        return random.randint(age_range[0], age_range[1])

    def _generate_random_value(self, value_range=VALUE_RANGE):
        return random.randint(value_range[0], value_range[1])

    def _generate_random_mval(self, mval_range=MVAL_RANGE):
        return random.randint(mval_range[0], mval_range[1])

    def _generate_random_social_value(self, sv_range=SOCIAL_VALUE_RANGE):
        return random.randint(sv_range[0], sv_range[1])

    def _generate_random_money_for_person(self, money_range=MONEY_RANGE_PERSON):
        # This is for setting initial money or for transactions.
        # For addMoney, the `num` can be positive or negative.
        # Let's assume this generates a delta for addMoney, or an absolute for init.
        return random.randint(money_range[0], money_range[1])

    def _generate_random_money_for_red_envelope(self, money_range=MONEY_RANGE_RED_ENVELOPE):
        return random.randint(money_range[0], money_range[1])

    def _generate_random_limit_dce(self, limit_range=LIMIT_RANGE_DCE):
        return random.randint(limit_range[0], limit_range[1])

    def _get_random_existing_person_id(self):
        existing_ids = self._get_existing_person_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_account_id(self):
        existing_ids = self._get_existing_account_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_article_id(self):
        existing_ids = self._get_existing_article_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_message_id(self):
        existing_ids = self._get_existing_message_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_stored_emoji_id(self):
        existing_ids = self._get_existing_stored_emoji_ids()
        return random.choice(existing_ids) if existing_ids else None

    def _get_random_existing_tag_id_for_person(self, person_id):  # Tag owned by person
        person_data = self.network_state["persons"].get(person_id)
        if not person_data: return None
        existing_tag_ids_owned = list(person_data.get("tags", set()))
        return random.choice(existing_tag_ids_owned) if existing_tag_ids_owned else None

    def _get_random_person_in_tag(self, owner_id, tag_id):
        member_ids = self._get_persons_in_tag(owner_id, tag_id)
        return random.choice(member_ids) if member_ids else None

    def _get_random_person_not_in_tag(self, owner_id, tag_id):
        existing_person_ids = self._get_existing_person_ids()
        members_in_tag = set(self._get_persons_in_tag(owner_id, tag_id))
        # Ensure the person itself is not chosen if it's the owner and not in its own tag members (which is normal)
        persons_not_in_tag = [pid for pid in existing_person_ids if pid not in members_in_tag]
        return random.choice(persons_not_in_tag) if persons_not_in_tag else None

    def _get_random_account_owned_by_person(self, person_id):
        owned_accounts = self._get_accounts_owned_by_person(person_id)
        return random.choice(owned_accounts) if owned_accounts else None

    def _get_random_account_not_owned_by_person(self, person_id):
        existing_account_ids = self._get_existing_account_ids()
        non_owned_accounts = [acc_id for acc_id in existing_account_ids if
                              self.network_state["accounts"][acc_id]["owner_id"] != person_id]
        return random.choice(non_owned_accounts) if non_owned_accounts else None

    def _get_random_follower_of_account(self, account_id):
        followers = self._get_followers_of_account(account_id)
        return random.choice(followers) if followers else None

    def _get_random_non_follower_of_account(self, account_id):
        existing_person_ids = self._get_existing_person_ids()
        followers = set(self._get_followers_of_account(account_id))
        non_followers = [pid for pid in existing_person_ids if pid not in followers]
        return random.choice(non_followers) if non_followers else None

    def _get_random_article_of_account(self, account_id):  # Article created by this account
        articles = self._get_articles_of_account(account_id)
        return random.choice(articles) if articles else None

    def _get_random_article_not_of_account(self, account_id):  # Article not created by this account
        existing_articles = self._get_existing_article_ids()
        account_articles = set(self._get_articles_of_account(account_id))
        non_account_articles = [aid for aid in existing_articles if aid not in account_articles]
        return random.choice(non_account_articles) if non_account_articles else None

    def _get_random_article_received_by_person(self, person_id):
        received_articles = self._get_articles_received_by_person(person_id)
        return random.choice(received_articles) if received_articles else None

    def _get_random_article_not_received_by_person(self, person_id):
        all_articles = self._get_existing_article_ids()
        received_by_person = set(self._get_articles_received_by_person(person_id))
        not_received = [aid for aid in all_articles if aid not in received_by_person]
        return random.choice(not_received) if not_received else None

    # --- Command Generators (Ported methods - HW10 ones are largely unchanged unless dependencies changed) ---
    # ... (ap, ar, mr, at, dt, att, dft, qv, qci, qts, qtav, qba, ln, coa, doa, ca, da, foa, qsp, qbc, qra, qtvs, qcs) ...
    # These are long, so I'll indicate where the new HW11 command generators start.
    # Assume the HW10 generators from the previous iteration are here.
    # I will paste them from my memory of the previous state if you need them explicitly,
    # but for brevity, I'll skip pasting them again right now unless you confirm.

    # --- START OF HW10 GENERATORS (placeholder, assume they exist as before) ---
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
            else:
                return None, None, None
        elif target_key is None:
            _id = self._get_random_non_existent_id("person")
            if _id is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

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
        elif target_key == ("ar", "ERE"):
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if
                                i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = OUTCOME_NORMAL
            elif len(existing_ids) < 2 and self.instructions_generated < self.target_instructions - 5:
                res1 = self._generate_ap(target_key=None)
                if res1: self._update_state_ap(res1[1])
                res2 = self._generate_ap(target_key=None)
                if res2: self._update_state_ap(res2[1])
                if res1 and res2:
                    id1, id2 = res1[1]['id'], res2[1]['id']
                    outcome = OUTCOME_NORMAL
                else:
                    return None, None, None
            else:
                return None, None, None
        else:
            return None, None, None

        if id1 is None or id2 is None: return None, None, None
        if id1 == id2: return None, None, None  # Ensure distinct IDs for normal/ERE/PINF cases

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
            if len(existing_ids) < 1: return None, None, None

            if target_key == ("mr", "PINF_id1"):
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
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
            else:
                return None, None, None
        elif target_key == ("mr", "RNF"):
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if
                                i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
            tag_id = self._generate_random_id("tag")  # Can be any tag id
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("at", "ETI"):
            persons_with_tags_owned = [pid for pid, data in state["persons"].items() if data.get("tags")]
            if persons_with_tags_owned:
                person_id = random.choice(persons_with_tags_owned)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            if existing_ids:
                person_id = random.choice(existing_ids)
                tag_id = self._get_random_non_existent_tag_id_for_person(person_id)
                if tag_id is None:  # If pool exhausted or person has all tags in pool
                    # Try generating a completely new tag_id outside the pool if that's desired
                    # For now, assume failure if specific non-existent cannot be found
                    return None, None, None
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
                if tag_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            persons_with_tags_owned = [pid for pid, data in state["persons"].items() if data.get("tags")]
            if persons_with_tags_owned:
                person_id = random.choice(persons_with_tags_owned)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is None: return None, None, None
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

        if person_id is None or tag_id is None: return None, None, None

        params = {"person_id": person_id, "tag_id": tag_id}
        cmd_str = f"dt {person_id} {tag_id}"
        return cmd_str, params, outcome

    def _generate_att(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None  # id1 = to_add, id2 = tag_owner
        tag_id = None
        params = {}
        outcome = None

        if target_key and target_key[0] == "att" and "PINF" in target_key[1]:
            non_existent_id = self._get_random_non_existent_id("person")
            if non_existent_id is None: return None, None, None
            if len(existing_ids) == 0: return None, None, None

            if target_key == ("att", "PINF_p1"):  # person to add (id1) not found
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None: return None, None, None
            elif target_key == ("att", "PINF_p2"):  # tag owner (id2) not found
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None
            tag_id = self._generate_random_id("tag")
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("att", "EPI_id1_eq_id2"):  # person1 == person2 (id1=id2)
            if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = id1
                tag_id = self._generate_random_id("tag")  # Any tag ID
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("att", "RNF"):  # Not linked
            valid_attempts = []
            # Find p1, p2 not linked, but p2 owns a tag
            for p2_id in existing_ids:
                p2_tags = self._get_existing_tag_ids_for_person(p2_id)
                if not p2_tags: continue
                tag_id_cand = random.choice(p2_tags)
                for p1_id in existing_ids:
                    if p1_id != p2_id and (min(p1_id, p2_id), max(p1_id, p2_id)) not in state["relations"]:
                        valid_attempts.append((p1_id, p2_id, tag_id_cand))
                        break
                if valid_attempts: break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("att", "TINF"):  # Tag not found for id2
            valid_attempts = []
            # Find p1, p2 linked, but p2 does not own the chosen tag_id
            linked_pairs = list(state["relations"].keys())
            random.shuffle(linked_pairs)
            for min_r_id, max_r_id in linked_pairs:
                p1_cand, p2_cand = random.choice([(min_r_id, max_r_id), (max_r_id, min_r_id)])
                if p1_cand == p2_cand: continue
                tag_id_cand = self._get_random_non_existent_tag_id_for_person(p2_cand)
                if tag_id_cand is not None:
                    valid_attempts.append((p1_cand, p2_cand, tag_id_cand))
                    break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("att", "EPI_in_tag"):  # id1 already in tag
            valid_attempts = []
            # Find p1, p2 linked, p2 owns tag_id, and p1 is already a member of (p2, tag_id)
            for (owner_id, tid), members in state["person_tags"].items():
                if not members: continue
                p2_id_cand = owner_id
                tag_id_cand = tid
                member_to_add_cand = random.choice(list(members.keys()))  # This is id1
                # Ensure p2_id_cand and member_to_add_cand are linked
                if member_to_add_cand != p2_id_cand and (
                min(member_to_add_cand, p2_id_cand), max(member_to_add_cand, p2_id_cand)) in state["relations"]:
                    valid_attempts.append((member_to_add_cand, p2_id_cand, tag_id_cand))
                    break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:  # Try normal
            valid_attempts = []
            # Find p1, p2 linked, p2 owns tag_id, p1 not in (p2, tag_id), tag not full
            linked_pairs = list(state["relations"].keys())
            random.shuffle(linked_pairs)
            for min_r_id, max_r_id in linked_pairs:
                p1_cand, p2_cand = random.choice([(min_r_id, max_r_id), (max_r_id, min_r_id)])
                if p1_cand == p2_cand: continue
                p2_owned_tags = self._get_existing_tag_ids_for_person(p2_cand)
                random.shuffle(p2_owned_tags)
                for tag_id_cand in p2_owned_tags:
                    tag_current_members = self._get_persons_in_tag(p2_cand, tag_id_cand)
                    if p1_cand not in tag_current_members and len(tag_current_members) < TAG_PERSONS_LIMIT:
                        valid_attempts.append((p1_cand, p2_cand, tag_id_cand))
                        break
                if valid_attempts: break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

        if id1 is None or id2 is None or tag_id is None: return None, None, None

        params = {"id1": id1, "id2": id2, "tag_id": tag_id}
        cmd_str = f"att {id1} {id2} {tag_id}"
        return cmd_str, params, outcome

    def _generate_dft(self, target_key=None):
        state = self.network_state
        existing_ids = self._get_existing_person_ids()
        id1, id2 = None, None  # id1 = to_delete, id2 = tag_owner
        tag_id = None
        params = {}
        outcome = None

        if target_key and target_key[0] == "dft" and "PINF" in target_key[1] and target_key != (
        "dft", "PINF_not_in_tag"):
            if len(existing_ids) < 1: return None, None, None
            if target_key == ("dft", "PINF_p1"):
                non_existent_id = self._get_random_non_existent_id("person")
                if non_existent_id is None: return None, None, None
                id1 = non_existent_id
                id2 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                if id2 is None: return None, None, None
                tag_id = self._generate_random_id("tag")
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            elif target_key == ("dft", "PINF_p2"):
                non_existent_id = self._get_random_non_existent_id("person")
                if non_existent_id is None: return None, None, None
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None
                tag_id = self._generate_random_id("tag")
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None  # Should not happen if key matches

        elif target_key == ("dft", "TINF"):  # Tag not found for id2
            if len(existing_ids) < 1: return None, None, None
            id1 = random.choice(existing_ids)  # Could be any existing person
            id2 = random.choice(existing_ids)  # Owner
            tag_id = self._get_random_non_existent_tag_id_for_person(id2)  # Tag id2 does not own
            if tag_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("dft", "PINF_not_in_tag"):  # id1 (person to delete) not in tag
            valid_attempts = []
            # Find p2 who owns a tag_id, and p1 who is NOT in that tag
            for p2_id_cand in existing_ids:
                p2_owned_tags = self._get_existing_tag_ids_for_person(p2_id_cand)
                if not p2_owned_tags: continue
                tag_id_cand = random.choice(p2_owned_tags)
                tag_members = self._get_persons_in_tag(p2_id_cand, tag_id_cand)

                non_members = [pid for pid in existing_ids if
                               pid not in tag_members and pid != p2_id_cand]  # pid1 != pid2 for dft JML usually
                if non_members:
                    p1_id_cand = random.choice(non_members)
                    # Ensure p1 and p2 exist and p2 owns tag
                    if state["persons"].get(p1_id_cand) and state["persons"].get(p2_id_cand) and tag_id_cand in \
                            state["persons"][p2_id_cand].get("tags", set()):
                        valid_attempts.append((p1_id_cand, p2_id_cand, tag_id_cand))
                        break
                if valid_attempts: break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:  # Try normal
            valid_attempts = []
            # Find p2 who owns tag_id, and p1 who IS in that tag
            for (owner_id, tid), members_map in state["person_tags"].items():
                if members_map:  # If tag has members
                    p2_id_cand = owner_id
                    tag_id_cand = tid
                    p1_id_cand = random.choice(list(members_map.keys()))
                    valid_attempts.append((p1_id_cand, p2_id_cand, tag_id_cand))
                    break
            if valid_attempts:
                id1, id2, tag_id = valid_attempts[0]
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
            non_linked_pairs = [(i, j) for i in existing_ids for j in existing_ids if
                                i != j and (min(i, j), max(i, j)) not in state["relations"]]
            if non_linked_pairs:
                id1, id2 = random.choice(non_linked_pairs)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            linked_pairs = list(state["relations"].keys())
            if linked_pairs:
                min_id, max_id = random.choice(linked_pairs)
                id1, id2 = random.choice([(min_id, max_id), (max_id, min_id)])
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
                if id2 is None: return None, None, None
            elif target_key == ("qci", "PINF_id2"):
                id1 = random.choice(existing_ids) if existing_ids else self._get_random_non_existent_id("person")
                id2 = non_existent_id
                if id1 is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None:
            if existing_ids:
                id1 = random.choice(existing_ids)
                id2 = random.choice(existing_ids)  # Can be same
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
        person_id = None  # This is the tag owner
        tag_id = None
        params = {}
        outcome = None

        if target_key == ("qtav", "PINF"):  # Person (tag owner) not found
            person_id = self._get_random_non_existent_id("person")
            tag_id = self._generate_random_id("tag")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("qtav", "TINF"):  # Tag not found for this person
            if existing_ids:
                person_id = random.choice(existing_ids)
                tag_id = self._get_random_non_existent_tag_id_for_person(person_id)  # Tag person_id does not own
                if tag_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:  # Try normal
            # Find a person who owns a tag that has members
            persons_owning_tags_with_members = []
            for (owner_id, tid), members_map in state["person_tags"].items():
                if members_map:  # If tag has members
                    persons_owning_tags_with_members.append((owner_id, tid))

            if persons_owning_tags_with_members:
                person_id, tag_id = random.choice(persons_owning_tags_with_members)
                outcome = OUTCOME_NORMAL
            else:  # No tags with members exist
                # Fallback: find any owned tag, even if empty (JML allows ageVar for empty tag -> 0)
                persons_owning_any_tag = [pid for pid, data in state["persons"].items() if data.get("tags")]
                if persons_owning_any_tag:
                    person_id = random.choice(persons_owning_any_tag)
                    tag_id = self._get_random_existing_tag_id_for_person(person_id)
                    if tag_id is not None:
                        outcome = OUTCOME_NORMAL
                    else:
                        return None, None, None  # Should not happen if person_id was from list
                else:
                    return None, None, None  # No one owns any tags
        else:
            return None, None, None

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
            persons_with_no_acquaintances = [pid for pid, data in state["persons"].items() if
                                             not data.get("acquaintances", {})]
            if persons_with_no_acquaintances:
                _id = random.choice(persons_with_no_acquaintances)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            persons_with_acquaintances = [pid for pid, data in state["persons"].items() if
                                          data.get("acquaintances", {})]
            if persons_with_acquaintances:
                _id = random.choice(persons_with_acquaintances)
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

        if _id is None: return None, None, None

        params = {"id": _id}
        cmd_str = f"qba {_id}"
        return cmd_str, params, outcome

    def _generate_ln(self, target_key=None):
        if target_key is not None: return None, None, None
        state = self.network_state
        self._initialize_state()  # Resets internal state of generator
        state = self.network_state  # Get the *newly initialized* state reference

        max_n = self.max_n_load_limit
        min_n = 1 if max_n > 0 else 0
        id_pool_size = ID_POOL_RANGE[1] - ID_POOL_RANGE[0] + 1
        if max_n > id_pool_size:
            max_n = id_pool_size
            if min_n > max_n: min_n = max_n
        if max_n <= 0:
            n = 0
        else:
            target_n = max(min_n, min(max_n, self.target_instructions // 10))
            n = random.randint(min_n, max(min_n, target_n))

        ids = []
        if n > 0:
            id_pool = list(range(ID_POOL_RANGE[0], ID_POOL_RANGE[1] + 1))
            if len(id_pool) < n: n = len(id_pool)
            if n > 0: ids = random.sample(id_pool, n)

        names = [self._generate_random_name() for _ in range(n)]
        ages = [self._generate_random_age() for _ in range(n)]
        values_matrix = []
        if n > 1:
            for i in range(n - 1):
                row = [random.randint(0, VALUE_RANGE[1]) for _ in range(i + 1)]
                values_matrix.append(row)

        output_lines = [f"ln {n}"]
        output_lines.append(" ".join(map(str, ids)) if n > 0 else "")
        output_lines.append(" ".join(names) if n > 0 else "")
        output_lines.append(" ".join(map(str, ages)) if n > 0 else "")
        if n > 1:
            for row in values_matrix: output_lines.append(" ".join(map(str, row)))
        output_str = "\n".join(line for line in output_lines if line is not None)

        # Update state based on ln (critical!)
        for i in range(n):
            person_id = ids[i]
            # Use _update_state_ap's logic for consistency but without exception
            state["persons"][person_id] = {
                "name": names[i],
                "age": ages[i],
                "acquaintances": {},
                "tags": set(),  # Person owns these tags
                "money": 0,  # Initialize money
                "socialValue": 0,  # Initialize socialValue
                "messages_received_obj": deque(),  # Initialize received messages
                "articles_received_ids": deque()  # Initialize received articles
            }
            # Note: person_tags stores members, not owned tags by person. Owned tags are in person[id]["tags"]

        if n > 1:
            for i in range(n - 1):
                for j in range(i + 1):
                    id1 = ids[i + 1]
                    id2 = ids[j]
                    value = values_matrix[i][j]
                    if value > 0:
                        # Use _update_state_ar's logic for consistency but without exception
                        pair_key = (min(id1, id2), max(id1, id2))
                        if id1 in state["persons"] and id2 in state["persons"] and id1 != id2 and pair_key not in state[
                            "relations"]:
                            state["persons"][id1]["acquaintances"][id2] = value
                            state["persons"][id2]["acquaintances"][id1] = value
                            state["relations"][pair_key] = value

        state["triple_sum"] = 0
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
        return output_str, params, OUTCOME_NORMAL

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
            account_id = self._generate_random_id("account", used_ids=existing_accounts)  # Try to get a new one
            if person_id is None or account_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("coa", "EOAI"):
            if existing_persons and existing_accounts:
                person_id = random.choice(existing_persons)
                account_id = random.choice(existing_accounts)
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                if account_id is None: return None, None, None
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
            if account_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("doa", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                if account_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("doa", "DAPermissionDenied_DOA"):
            # Find account owned by someone else
            accounts_and_owners = [(acc_id, data["owner_id"]) for acc_id, data in state["accounts"].items()]
            random.shuffle(accounts_and_owners)
            found = False
            for acc_id_cand, owner_id_cand in accounts_and_owners:
                non_owners = [pid for pid in existing_persons if pid != owner_id_cand]
                if non_owners:
                    person_id = random.choice(non_owners)
                    account_id = acc_id_cand
                    found = True
                    break
            if found:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            # Find account owned by person_id
            persons_owning_accounts = [pid for pid in existing_persons if self._get_accounts_owned_by_person(pid)]
            if persons_owning_accounts:
                person_id = random.choice(persons_owning_accounts)
                owned_accounts = self._get_accounts_owned_by_person(person_id)  # Should not be empty
                if owned_accounts:
                    account_id = random.choice(owned_accounts)
                    outcome = OUTCOME_NORMAL
                else:
                    return None, None, None  # Should not happen
            else:
                return None, None, None
        else:
            return None, None, None

        if person_id is None or account_id is None: return None, None, None

        params = {"person_id": person_id, "account_id": account_id}
        cmd_str = f"doa {person_id} {account_id}"
        return cmd_str, params, outcome

    def _generate_ca(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        existing_accounts = self._get_existing_account_ids()
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
            article_id = self._get_random_non_existent_id("article")
            if account_id is None or article_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("ca", "OAINF"):
            if existing_persons:
                person_id = random.choice(existing_persons)
                account_id = self._get_random_non_existent_id("account")
                article_id = self._get_random_non_existent_id("article")
                if account_id is None or article_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("ca", "EAI"):  # Equal Article ID
            # Need existing person, existing account, person is follower, article_id already exists IN NETWORK
            accounts_with_followers = [(acc_id, self._get_followers_of_account(acc_id)) for acc_id in existing_accounts]
            random.shuffle(accounts_with_followers)
            found = False
            for acc_id_cand, followers_list in accounts_with_followers:
                if followers_list and existing_persons:  # Ensure followers_list is not empty
                    # Pick an existing article that this account *does not* yet have (to avoid other errors)
                    # Or any existing article if EAI is the sole focus
                    all_network_articles = self._get_existing_article_ids()
                    if all_network_articles:
                        article_id = random.choice(all_network_articles)
                        person_id = random.choice(followers_list)
                        account_id = acc_id_cand
                        found = True
                        break
            if found:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("ca", "ContributePermissionDenied"):
            # Person not a follower of the account
            accounts_with_non_followers = []
            for acc_id_cand in existing_accounts:
                non_followers = self._get_random_non_follower_of_account(acc_id_cand)
                if non_followers:
                    accounts_with_non_followers.append((acc_id_cand, non_followers))

            if accounts_with_non_followers:
                account_id, person_id_cand = random.choice(accounts_with_non_followers)
                person_id = person_id_cand  # This is actually a list of one from helper
                article_id = self._get_random_non_existent_id("article")  # New article
                if person_id is None or article_id is None: return None, None, None
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            # Person is follower, account exists, article is new
            accounts_with_followers = [(acc_id, self._get_followers_of_account(acc_id)) for acc_id in existing_accounts]
            random.shuffle(accounts_with_followers)
            found = False
            for acc_id_cand, followers_list in accounts_with_followers:
                if followers_list:
                    person_id = random.choice(followers_list)
                    account_id = acc_id_cand
                    article_id = self._get_random_non_existent_id("article")
                    if article_id is None: continue  # Could not generate new article id
                    found = True
                    break
            if found:
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

        if person_id is None or account_id is None or article_id is None: return None, None, None

        # For ca, the JML implies name is not part of the call, Runner handles it internally.
        # Our state update will need a dummy name.
        params = {"person_id": person_id, "account_id": account_id, "article_id": article_id,
                  "article_name_dummy": "DummyArticleName"}
        cmd_str = f"ca {person_id} {account_id} {article_id}"  # Name not in command
        return cmd_str, params, outcome

    def _generate_da(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        person_id = None
        account_id = None
        article_id = None
        params = {}
        outcome = None

        if target_key == ("da", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            account_id = self._get_random_existing_account_id()
            article_id = self._get_random_existing_article_id()
            if person_id is None or account_id is None or article_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("da", "OAINF"):
            person_id = self._get_random_existing_person_id()
            account_id = self._get_random_non_existent_id("account")
            article_id = self._get_random_existing_article_id()
            if person_id is None or account_id is None or article_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("da", "AINF"):  # Article not in account
            # Find account with owner, and an article NOT in that account's list
            accounts_with_owners = [(acc_id, data["owner_id"]) for acc_id, data in state["accounts"].items()]
            random.shuffle(accounts_with_owners)
            found = False
            for acc_id_cand, owner_id_cand in accounts_with_owners:
                article_not_in_acc = self._get_random_article_not_of_account(acc_id_cand)
                if article_not_in_acc is not None:  # Ensure such an article exists in the network generally
                    person_id = owner_id_cand
                    account_id = acc_id_cand
                    article_id = article_not_in_acc
                    found = True
                    break
            if found:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key == ("da", "DAPermissionDenied_DA"):  # Person not owner of account
            # Find account with article, and a person who is NOT the owner
            accounts_with_articles_and_owners = [
                (acc_id, data["owner_id"], list(data["articles"]))
                for acc_id, data in state["accounts"].items() if data["articles"]
            ]
            random.shuffle(accounts_with_articles_and_owners)
            found = False
            for acc_id_cand, owner_id_cand, articles_list in accounts_with_articles_and_owners:
                non_owners = [pid for pid in existing_persons if pid != owner_id_cand]
                if non_owners:
                    person_id = random.choice(non_owners)
                    account_id = acc_id_cand
                    article_id = random.choice(articles_list)
                    found = True
                    break
            if found:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            # Person is owner, account has article
            accounts_with_articles_and_owners = [
                (acc_id, data["owner_id"], list(data["articles"]))
                for acc_id, data in state["accounts"].items() if data["articles"]
            ]
            random.shuffle(accounts_with_articles_and_owners)
            found = False
            for acc_id_cand, owner_id_cand, articles_list in accounts_with_articles_and_owners:
                person_id = owner_id_cand
                account_id = acc_id_cand
                article_id = random.choice(articles_list)
                found = True
                break
            if found:
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
            person_id = self._get_random_non_existent_id("person")
            account_id = random.choice(existing_accounts) if existing_accounts else self._generate_random_id("account")
            if person_id is None or account_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("foa", "OAINF"):
            person_id = random.choice(existing_persons) if existing_persons else self._get_random_non_existent_id(
                "person")
            account_id = self._get_random_non_existent_id("account")
            if person_id is None or account_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key == ("foa", "EPI_follower"):  # Person already a follower
            # Find account with followers
            accounts_with_followers = [(acc_id, self._get_followers_of_account(acc_id)) for acc_id in existing_accounts]
            random.shuffle(accounts_with_followers)
            found = False
            for acc_id_cand, followers_list in accounts_with_followers:
                if followers_list:
                    person_id = random.choice(followers_list)
                    account_id = acc_id_cand
                    found = True
                    break
            if found:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None
        elif target_key is None:
            # Person not a follower of account
            accounts_with_non_followers = []
            for acc_id_cand in existing_accounts:
                non_follower = self._get_random_non_follower_of_account(acc_id_cand)  # Returns one non-follower
                if non_follower:
                    accounts_with_non_followers.append((acc_id_cand, non_follower))

            if accounts_with_non_followers:
                account_id, person_id_cand = random.choice(accounts_with_non_followers)
                person_id = person_id_cand
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
            # Try finding two distinct components
            all_persons = list(existing_ids)
            random.shuffle(all_persons)
            for start_node_cand in all_persons:
                reachable_set = self._bfs_reachable(start_node_cand)
                unreachable_candidates = [pid for pid in existing_ids if pid not in reachable_set]
                if unreachable_candidates:
                    id1 = start_node_cand
                    id2 = random.choice(unreachable_candidates)
                    found_unreachable = True
                    break
            if found_unreachable:
                outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
            else:
                return None, None, None

        elif target_key is None:
            if not existing_ids: return None, None, None
            id1 = random.choice(existing_ids)
            # For normal, try to pick a reachable one, or id1 itself (path length 0)
            reachable_set = self._bfs_reachable(id1)
            if reachable_set:  # Should always include id1
                id2 = random.choice(list(reachable_set))
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
        elif target_key is None:
            if existing_accounts:
                account_id = random.choice(existing_accounts)
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

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
        elif target_key is None:
            if existing_persons:
                person_id = random.choice(existing_persons)
                outcome = OUTCOME_NORMAL
            else:
                return None, None, None
        else:
            return None, None, None

        if person_id is None: return None, None, None

        params = {"person_id": person_id}
        cmd_str = f"qra {person_id}"
        return cmd_str, params, outcome

    def _generate_qtvs(self, target_key=None):
        state = self.network_state
        existing_persons = self._get_existing_person_ids()
        person_id = None  # Tag owner
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
            else:
                return None, None, None
        elif target_key is None:
            # Find person owning a tag (even if empty, sum is 0)
            persons_owning_any_tag = [pid for pid, data in state["persons"].items() if data.get("tags")]
            if persons_owning_any_tag:
                person_id = random.choice(persons_owning_any_tag)
                tag_id = self._get_random_existing_tag_id_for_person(person_id)
                if tag_id is not None:
                    outcome = OUTCOME_NORMAL
                else:
                    return None, None, None
            else:
                return None, None, None
        else:
            return None, None, None

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

    # --- END OF HW10 GENERATORS (placeholder) ---

    # --- HW11 New Command Generators ---

    def _generate_message_object_structure(self, msg_id, type_val, social_value, p1_id, p2_id, tag_id, msg_kind,
                                           emoji_id=None, lucky_money=None, article_id=None):
        """Helper to create the internal message object dictionary."""
        return {
            "id": msg_id,
            "type": type_val,  # 0 for person-to-person, 1 for group
            "socialValue": social_value,  # Derived or given
            "person1_id": p1_id,  # Sender
            "person2_id": p2_id,  # Receiver (if type 0)
            "tag_id": tag_id,  # Tag ID (if type 1)
            "msg_kind": msg_kind,  # "ordinary", "emoji", "red_envelope", "forward"
            "emojiId": emoji_id,  # Specific to EmojiMessage
            "lucky_money": lucky_money,  # Specific to RedEnvelopeMessage
            "articleId": article_id,  # Specific to ForwardMessage
        }

    def _generate_add_ordinary_message(self, target_key=None):  # Corresponds to 'am'
        state = self.network_state
        msg_id = self._generate_random_id("message", used_ids=list(self.network_state["all_message_ids_ever_used"]))
        if msg_id is None: return None, None, None  # Cannot generate new message ID

        social_value_param = self._generate_random_social_value()  # Input social value for am

        # Determine type (0 or 1)
        msg_type = random.choice([0, 1])

        p1_id, p2_id, tag_id_for_group = None, None, None
        outcome = None
        params = {}

        # --- Target specific exceptions for Network.addMessage related to EqualMessageIdException ---
        if target_key == ("am", "EMIE"):  # EqualMessageId
            if not self._get_existing_message_ids(): return None, None, None  # Need an existing msg to collide
            msg_id = random.choice(self._get_existing_message_ids())
            # For this exception, other params can be valid or invalid, EMIE is checked first by JML
            # Let's try to make them valid to isolate EMIE
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:  # type 1
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        # --- Target specific exceptions for Network.addMessage related to EqualPersonIdException ---
        elif target_key == ("am", "EPI_msg"):  # type=0, p1 == p2
            if not self._get_existing_person_ids(): return None, None, None
            p1_id = random.choice(self._get_existing_person_ids())
            p2_id = p1_id
            msg_type = 0  # Must be type 0 for this specific JML EPI
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        # --- Normal Case or other exceptions (Runner might catch PINF/TINF first) ---
        elif target_key is None:  # Try normal
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None

            if msg_type == 0:  # Person-to-person
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None  # Not enough people for type 0
                p2_id = random.choice(p2_id_candidates)
            else:  # type 1, group message
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None  # p1 owns no tags for group message
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None  # Other specific exceptions not handled for 'am' here as they are for specialized messages

        if p1_id is None or (msg_type == 0 and p2_id is None) or (msg_type == 1 and tag_id_for_group is None):
            return None, None, None

        # Create the message object for state update (socialValue is param for ordinary)
        # Runner actually creates the concrete Message object. We simulate its properties.
        # JML socialValue for ordinary message is not derived, it's taken from input.
        # socialValue of the message object itself for ordinary message IS social_value_param

        params = {
            "id": msg_id, "socialValue": social_value_param, "type": msg_type,
            "person1_id": p1_id,
            "person2_id": p2_id if msg_type == 0 else None,  # Command needs target person/tag id
            "tag_id_param": tag_id_for_group if msg_type == 1 else (p2_id if msg_type == 0 else None)
            # The 5th param of 'am'
        }

        # The command string's 5th argument is person_id2 or tag_id
        cmd_arg5 = p2_id if msg_type == 0 else tag_id_for_group
        cmd_str = f"am {msg_id} {social_value_param} {msg_type} {p1_id} {cmd_arg5}"

        # The internal message object to be stored
        internal_msg_obj_params = {
            "msg_id": msg_id, "type_val": msg_type, "social_value": social_value_param,
            "p1_id": p1_id, "p2_id": p2_id, "tag_id": tag_id_for_group,
            "msg_kind": "ordinary"
        }
        # Attach the internal object structure to params for _update_state_add_message
        params["internal_message_object_params"] = internal_msg_obj_params

        return cmd_str, params, outcome

    def _generate_add_emoji_message(self, target_key=None):  # Corresponds to 'aem'
        state = self.network_state
        msg_id = self._generate_random_id("message", used_ids=list(self.network_state["all_message_ids_ever_used"]))
        if msg_id is None: return None, None, None

        emoji_id_param = self._generate_random_id("emoji")  # The emojiId for the message content
        msg_type = random.choice([0, 1])
        p1_id, p2_id, tag_id_for_group = None, None, None
        outcome = None
        params = {}

        if target_key == ("aem", "EMIE"):  # EqualMessageId
            if not self._get_existing_message_ids(): return None, None, None
            msg_id = random.choice(self._get_existing_message_ids())
            # Make other params valid if possible
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            # Ensure emoji_id_param is a stored one for validity beyond EMIE
            emoji_id_param = self._get_random_existing_stored_emoji_id()
            if emoji_id_param is None:  # if no emoji stored, this test might not be pure EMIE
                # for now, let it pass, EMIE is primary
                emoji_id_param = self._generate_random_id("emoji")  # pick any

            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("aem", "EMINFE"):  # EmojiIdNotFound (emoji_id_param not stored in network)
            emoji_id_param = self._get_random_non_existent_id("emoji")  # An emoji ID not in network.emojiIdList
            if emoji_id_param is None: return None, None, None  # Cannot find non-existent emoji
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("aem", "EPI_msg"):  # type=0, p1 == p2
            if not self._get_existing_person_ids(): return None, None, None
            p1_id = random.choice(self._get_existing_person_ids())
            p2_id = p1_id
            msg_type = 0
            emoji_id_param = self._get_random_existing_stored_emoji_id()  # Make emoji_id valid
            if emoji_id_param is None: emoji_id_param = self._generate_random_id("emoji")  # fallback
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None:  # Normal
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            emoji_id_param = self._get_random_existing_stored_emoji_id()  # Must be a stored emoji for normal add
            if emoji_id_param is None: return None, None, None  # Cannot add if no emojis stored

            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if p1_id is None or (msg_type == 0 and p2_id is None) or (msg_type == 1 and tag_id_for_group is None):
            return None, None, None

        # JML: socialValue == emojiId for EmojiMessage
        message_social_value = emoji_id_param

        params = {
            "id": msg_id, "emoji_id_content": emoji_id_param, "type": msg_type,
            "person1_id": p1_id,
            "person2_id": p2_id if msg_type == 0 else None,
            "tag_id_param": tag_id_for_group if msg_type == 1 else (p2_id if msg_type == 0 else None)
        }
        cmd_arg5 = p2_id if msg_type == 0 else tag_id_for_group
        cmd_str = f"aem {msg_id} {emoji_id_param} {msg_type} {p1_id} {cmd_arg5}"

        params["internal_message_object_params"] = self._generate_message_object_structure(
            msg_id, msg_type, message_social_value, p1_id, p2_id, tag_id_for_group,
            "emoji", emoji_id=emoji_id_param
        )
        return cmd_str, params, outcome

    def _generate_add_red_envelope_message(self, target_key=None):  # Corresponds to 'arem'
        state = self.network_state
        msg_id = self._generate_random_id("message", used_ids=list(self.network_state["all_message_ids_ever_used"]))
        if msg_id is None: return None, None, None

        lucky_money_param = self._generate_random_money_for_red_envelope()
        msg_type = random.choice([0, 1])
        p1_id, p2_id, tag_id_for_group = None, None, None
        outcome = None
        params = {}

        if target_key == ("arem", "EMIE"):
            if not self._get_existing_message_ids(): return None, None, None
            msg_id = random.choice(self._get_existing_message_ids())
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("arem", "EPI_msg"):  # type=0, p1 == p2
            if not self._get_existing_person_ids(): return None, None, None
            p1_id = random.choice(self._get_existing_person_ids())
            p2_id = p1_id
            msg_type = 0
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None:  # Normal
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if p1_id is None or (msg_type == 0 and p2_id is None) or (msg_type == 1 and tag_id_for_group is None):
            return None, None, None

        # JML: socialValue == money * 5 for RedEnvelopeMessage
        message_social_value = lucky_money_param * 5

        params = {
            "id": msg_id, "lucky_money_content": lucky_money_param, "type": msg_type,
            "person1_id": p1_id,
            "person2_id": p2_id if msg_type == 0 else None,
            "tag_id_param": tag_id_for_group if msg_type == 1 else (p2_id if msg_type == 0 else None)
        }
        cmd_arg5 = p2_id if msg_type == 0 else tag_id_for_group
        cmd_str = f"arem {msg_id} {lucky_money_param} {msg_type} {p1_id} {cmd_arg5}"

        params["internal_message_object_params"] = self._generate_message_object_structure(
            msg_id, msg_type, message_social_value, p1_id, p2_id, tag_id_for_group,
            "red_envelope", lucky_money=lucky_money_param
        )
        return cmd_str, params, outcome

    def _generate_add_forward_message(self, target_key=None):  # Corresponds to 'afm'
        state = self.network_state
        msg_id = self._generate_random_id("message", used_ids=list(self.network_state["all_message_ids_ever_used"]))
        if msg_id is None: return None, None, None

        article_id_param = None  # This will be chosen carefully
        msg_type = random.choice([0, 1])
        p1_id, p2_id, tag_id_for_group = None, None, None
        outcome = None
        params = {}

        if target_key == ("afm", "EMIE"):
            if not self._get_existing_message_ids(): return None, None, None
            msg_id = random.choice(self._get_existing_message_ids())
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            # For EMIE, article_id must be valid (in network AND in p1's received)
            article_id_param = self._get_random_article_received_by_person(p1_id)
            if article_id_param is None:  # p1 has no received articles
                # Try to find any article in network if p1 has none
                article_id_param = self._get_random_existing_article_id()
                if article_id_param is None: return None, None, None  # No articles at all

            if msg_type == 0:
                p2_id_cand = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_cand: return None, None, None
                p2_id = random.choice(p2_id_cand)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("afm", "AINFE_net"):  # Article not in network.articles at all
            article_id_param = self._get_random_non_existent_id("article")  # Not in network
            if article_id_param is None: return None, None, None
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            if msg_type == 0:
                p2_id_cand = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_cand: return None, None, None
                p2_id = random.choice(p2_id_cand)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("afm", "AINFE_person"):  # Article in network, but not in p1's received
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            article_id_param = self._get_random_article_not_received_by_person(p1_id)
            if article_id_param is None:  # p1 has received all articles or no articles exist
                # Check if any article exists at all
                if not self._get_existing_article_ids(): return None, None, None  # No articles to pick from
                # If p1 has received all, this specific AINFE cannot be triggered easily.
                # Try to pick any existing article, assuming p1 hasn't received it.
                # This case is hard to guarantee if p1 tends to receive many articles.
                return None, None, None  # Skip if hard to setup

            if msg_type == 0:
                p2_id_cand = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_cand: return None, None, None
                p2_id = random.choice(p2_id_cand)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("afm", "EPI_msg"):  # type=0, p1 == p2
            if not self._get_existing_person_ids(): return None, None, None
            p1_id = random.choice(self._get_existing_person_ids())
            p2_id = p1_id
            msg_type = 0
            article_id_param = self._get_random_article_received_by_person(p1_id)  # Make article_id valid
            if article_id_param is None: article_id_param = self._get_random_existing_article_id()  # fallback
            if article_id_param is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None:  # Normal
            p1_id = self._get_random_existing_person_id()
            if not p1_id: return None, None, None
            article_id_param = self._get_random_article_received_by_person(p1_id)  # Must be received by p1
            if article_id_param is None: return None, None, None  # p1 has no articles to forward

            if msg_type == 0:
                p2_id_candidates = [pid for pid in self._get_existing_person_ids() if pid != p1_id]
                if not p2_id_candidates: return None, None, None
                p2_id = random.choice(p2_id_candidates)
            else:
                tag_id_for_group = self._get_random_existing_tag_id_for_person(p1_id)
                if tag_id_for_group is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if p1_id is None or article_id_param is None or \
                (msg_type == 0 and p2_id is None) or \
                (msg_type == 1 and tag_id_for_group is None):
            return None, None, None

        # JML: socialValue == abs(articleId) % 200
        message_social_value = abs(article_id_param) % 200

        params = {
            "id": msg_id, "article_id_content": article_id_param, "type": msg_type,
            "person1_id": p1_id,
            "person2_id": p2_id if msg_type == 0 else None,
            "tag_id_param": tag_id_for_group if msg_type == 1 else (p2_id if msg_type == 0 else None)
        }
        cmd_arg5 = p2_id if msg_type == 0 else tag_id_for_group
        cmd_str = f"afm {msg_id} {article_id_param} {msg_type} {p1_id} {cmd_arg5}"

        params["internal_message_object_params"] = self._generate_message_object_structure(
            msg_id, msg_type, message_social_value, p1_id, p2_id, tag_id_for_group,
            "forward", article_id=article_id_param
        )
        return cmd_str, params, outcome

    def _generate_sm(self, target_key=None):  # sendMessage
        state = self.network_state
        if not state["messages_map"]: return None, None, None  # No messages to send

        msg_to_send_id = None
        outcome = None
        params = {}

        if target_key == ("sm", "MINFE"):
            msg_to_send_id = self._get_random_non_existent_id("message")
            if msg_to_send_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("sm", "RNF_sm"):  # RelationNotFound for type 0 message
            # Find a type 0 message where p1 and p2 are NOT linked
            type0_messages_not_linked = []
            for mid, m_obj in state["messages_map"].items():
                if m_obj["type"] == 0:
                    p1 = m_obj["person1_id"]
                    p2 = m_obj["person2_id"]
                    if p1 and p2 and (min(p1, p2), max(p1, p2)) not in state["relations"]:
                        type0_messages_not_linked.append(mid)
            if not type0_messages_not_linked: return None, None, None
            msg_to_send_id = random.choice(type0_messages_not_linked)
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key == ("sm", "TINF_sm"):  # TagIdNotFound for type 1 message
            # Find a type 1 message where p1 does NOT own the tag
            type1_messages_tag_not_owned = []
            for mid, m_obj in state["messages_map"].items():
                if m_obj["type"] == 1:
                    p1 = m_obj["person1_id"]
                    tag = m_obj["tag_id"]
                    if p1 and tag and (tag not in state["persons"].get(p1, {}).get("tags", set())):
                        type1_messages_tag_not_owned.append(mid)
            if not type1_messages_tag_not_owned: return None, None, None
            msg_to_send_id = random.choice(type1_messages_tag_not_owned)
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]

        elif target_key is None:  # Normal send
            # Find a message that CAN be sent (type 0 linked, or type 1 tag owned)
            sendable_message_ids = []
            for mid, m_obj in state["messages_map"].items():
                p1 = m_obj["person1_id"]
                if m_obj["type"] == 0:
                    p2 = m_obj["person2_id"]
                    if p1 and p2 and (min(p1, p2), max(p1, p2)) in state["relations"]:
                        sendable_message_ids.append(mid)
                elif m_obj["type"] == 1:
                    tag = m_obj["tag_id"]
                    if p1 and tag and (tag in state["persons"].get(p1, {}).get("tags", set())):
                        sendable_message_ids.append(mid)
            if not sendable_message_ids: return None, None, None
            msg_to_send_id = random.choice(sendable_message_ids)
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if msg_to_send_id is None: return None, None, None

        params = {"id": msg_to_send_id}
        cmd_str = f"sm {msg_to_send_id}"
        return cmd_str, params, outcome

    def _generate_qsv(self, target_key=None):  # querySocialValue
        state = self.network_state
        person_id = None
        outcome = None
        params = {}

        if target_key == ("qsv", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None:  # Normal
            person_id = self._get_random_existing_person_id()
            if person_id is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if person_id is None: return None, None, None
        params = {"id": person_id}
        cmd_str = f"qsv {person_id}"
        return cmd_str, params, outcome

    def _generate_qrm(self, target_key=None):  # queryReceivedMessages
        state = self.network_state
        person_id = None
        outcome = None
        params = {}

        if target_key == ("qrm", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None:  # Normal
            person_id = self._get_random_existing_person_id()
            if person_id is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if person_id is None: return None, None, None
        params = {"id": person_id}
        cmd_str = f"qrm {person_id}"
        return cmd_str, params, outcome

    def _generate_sei(self, target_key=None):  # storeEmojiId
        state = self.network_state
        emoji_id_to_store = None
        outcome = None
        params = {}

        if target_key == ("sei", "EEIE"):  # EqualEmojiId
            emoji_id_to_store = self._get_random_existing_stored_emoji_id()
            if emoji_id_to_store is None: return None, None, None  # No emojis stored yet
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None:  # Normal
            emoji_id_to_store = self._generate_random_id("emoji", used_ids=self._get_existing_stored_emoji_ids())
            if emoji_id_to_store is None: return None, None, None  # Pool exhausted
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if emoji_id_to_store is None: return None, None, None
        params = {"id": emoji_id_to_store}
        cmd_str = f"sei {emoji_id_to_store}"
        return cmd_str, params, outcome

    def _generate_qp(self, target_key=None):  # queryPopularity
        state = self.network_state
        emoji_id_to_query = None
        outcome = None
        params = {}

        if target_key == ("qp", "EMINFE_qp"):  # EmojiIdNotFound for query
            emoji_id_to_query = self._get_random_non_existent_id("emoji")  # Not in network.emojiIdList
            if emoji_id_to_query is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None:  # Normal
            emoji_id_to_query = self._get_random_existing_stored_emoji_id()
            if emoji_id_to_query is None: return None, None, None  # No emojis stored to query
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if emoji_id_to_query is None: return None, None, None
        params = {"id": emoji_id_to_query}
        cmd_str = f"qp {emoji_id_to_query}"
        return cmd_str, params, outcome

    def _generate_dce(self, target_key=None):  # deleteColdEmoji
        # This command has no JML exceptions, always normal behavior.
        if target_key is not None: return None, None, None

        limit_param = self._generate_random_limit_dce()
        outcome = OUTCOME_NORMAL
        params = {"limit": limit_param}
        cmd_str = f"dce {limit_param}"
        return cmd_str, params, outcome

    def _generate_qm(self, target_key=None):  # queryMoney
        state = self.network_state
        person_id = None
        outcome = None
        params = {}

        if target_key == ("qm", "PINF"):
            person_id = self._get_random_non_existent_id("person")
            if person_id is None: return None, None, None
            outcome = GENERATOR_TARGET_OUTCOME_MAP[target_key]
        elif target_key is None:  # Normal
            person_id = self._get_random_existing_person_id()
            if person_id is None: return None, None, None
            outcome = OUTCOME_NORMAL
        else:
            return None, None, None

        if person_id is None: return None, None, None
        params = {"id": person_id}
        cmd_str = f"qm {person_id}"
        return cmd_str, params, outcome

    # --- State Update Functions (Ported methods - HW10 ones are largely unchanged unless dependencies changed) ---
    # ... (ap, ar, mr, at, dt, att, dft, coa, doa, ca, da, foa) ...
    # Query commands and qts, qcs, ln do not have separate update methods here as their state impact is either none or handled within their generator (_generate_ln)

    # --- START OF HW10 STATE UPDATERS (placeholder) ---
    def _update_state_ap(self, params):
        state = self.network_state
        _id = params["id"]
        if _id not in state["persons"]:  # Should be checked by JML via containsPerson
            state["persons"][_id] = {
                "name": params["name"],
                "age": params["age"],
                "acquaintances": {},
                "tags": set(),  # Person owns these tags
                "money": 0,
                "socialValue": 0,
                "messages_received_obj": deque(),
                "articles_received_ids": deque()
            }
            state["couple_sum_dirty"] = True

    def _update_state_ar(self, params):
        state = self.network_state
        id1, id2, value = params["id1"], params["id2"], params["value"]
        pair = (min(id1, id2), max(id1, id2))
        # Assuming checks for person existence and not already linked are passed (JML)
        if id1 in state["persons"] and id2 in state["persons"] and id1 != id2 and pair not in state["relations"]:
            state["persons"][id1]["acquaintances"][id2] = value
            state["persons"][id2]["acquaintances"][id1] = value
            state["relations"][pair] = value

            # Efficiently update triple_sum (copied from _generate_ln, adjust for single edge)
            # This logic is complex and error-prone, ensure it matches JML's definition.
            # For now, just recalculate for simplicity if needed, or mark dirty.
            # The original generator's triple sum update in 'ar' was complex.
            # Let's defer full recalculation and just mark dirty. A full qts will do it.
            # For a more precise update:
            new_triangles = 0
            p1_acq = set(state["persons"][id1]["acquaintances"].keys()) - {id2}
            p2_acq = set(state["persons"][id2]["acquaintances"].keys()) - {id1}
            common_neighbors_of_new_pair = p1_acq.intersection(p2_acq)
            new_triangles += len(common_neighbors_of_new_pair)
            state["triple_sum"] += new_triangles
            state["couple_sum_dirty"] = True

    def _update_state_mr(self, params):
        state = self.network_state
        id1, id2, m_val = params["id1"], params["id2"], params["m_val"]
        pair = (min(id1, id2), max(id1, id2))
        # Assuming checks (persons exist, linked, id1 != id2) are passed (JML)
        if pair in state["relations"]:
            old_value = state["relations"][pair]
            new_value = old_value + m_val

            # Triple sum adjustment before relation change/removal
            triangles_involving_edge = 0
            if old_value > 0:  # If it was an actual edge
                p1_acq_old = set(state["persons"][id1]["acquaintances"].keys()) - {id2}
                p2_acq_old = set(state["persons"][id2]["acquaintances"].keys()) - {id1}
                common_neighbors_old = p1_acq_old.intersection(p2_acq_old)
                triangles_involving_edge = len(common_neighbors_old)

            if new_value > 0:
                state["persons"][id1]["acquaintances"][id2] = new_value
                state["persons"][id2]["acquaintances"][id1] = new_value
                state["relations"][pair] = new_value
                # Triple sum doesn't change if value changes but edge remains.
            else:  # Relation removed
                del state["persons"][id1]["acquaintances"][id2]
                del state["persons"][id2]["acquaintances"][id1]
                del state["relations"][pair]
                state["triple_sum"] -= triangles_involving_edge  # Subtract lost triangles

                # Side effect: remove from tags as per JML
                # This is complex as it involves iterating tags of id1 and id2.
                # For id1's tags: if id2 was in any of id1.tag[k], remove id2.
                # For id2's tags: if id1 was in any of id2.tag[k], remove id1.
                for tag_owner_id, tag_to_check_id, person_to_remove_from_tag in [(id1, id2), (id2, id1)]:
                    owner_tags = state["persons"][tag_owner_id].get("tags", set())
                    for owned_tag_id in list(owner_tags):  # Iterate copy if modifying
                        tag_key = (tag_owner_id, owned_tag_id)
                        if tag_key in state["person_tags"]:
                            if person_to_remove_from_tag in state["person_tags"][tag_key]:
                                del state["person_tags"][tag_key][person_to_remove_from_tag]
            state["couple_sum_dirty"] = True

    def _update_state_at(self, params):  # Add Tag to person
        state = self.network_state
        person_id, tag_id = params["person_id"], params["tag_id"]
        # Assuming person exists and doesn't already have this tag (JML checks)
        if person_id in state["persons"]:
            state["persons"][person_id].setdefault("tags", set()).add(tag_id)
            state["person_tags"][(person_id, tag_id)] = {}  # Initialize empty member list for this new tag instance

    def _update_state_dt(self, params):  # Delete Tag from person
        state = self.network_state
        person_id, tag_id = params["person_id"], params["tag_id"]
        # Assuming person exists and has this tag (JML checks)
        if person_id in state["persons"] and "tags" in state["persons"][person_id]:
            state["persons"][person_id]["tags"].discard(tag_id)
            if not state["persons"][person_id]["tags"]:  # if set becomes empty
                del state["persons"][person_id]["tags"]
        if (person_id, tag_id) in state["person_tags"]:
            del state["person_tags"][(person_id, tag_id)]  # Remove member list too

    def _update_state_att(self, params):  # Add Person To Tag
        state = self.network_state
        id1, id2, tag_id = params["id1"], params["id2"], params["tag_id"]  # p1 to add, p2 owns tag
        tag_key = (id2, tag_id)
        # Assuming JML checks passed (p1,p2 exist, p1!=p2, linked, p2 has tag, p1 not in tag, tag not full)
        if tag_key in state["person_tags"]:  # Ensure tag instance exists
            p1_age = state["persons"][id1]["age"]
            state["person_tags"][tag_key][id1] = p1_age

    def _update_state_dft(self, params):  # Delete Person From Tag
        state = self.network_state
        id1, id2, tag_id = params["id1"], params["id2"], params["tag_id"]  # p1 to delete, p2 owns tag
        tag_key = (id2, tag_id)
        # Assuming JML checks passed (p1,p2 exist, p2 has tag, p1 in tag)
        if tag_key in state["person_tags"] and id1 in state["person_tags"][tag_key]:
            del state["person_tags"][tag_key][id1]

    def _update_state_coa(self, params):
        state = self.network_state
        person_id, account_id, account_name = params["person_id"], params["account_id"], params["account_name"]
        # Assuming JML checks passed
        if person_id in state["persons"] and account_id not in state["accounts"]:
            state["accounts"][account_id] = {
                "owner_id": person_id, "name": account_name,
                "followers": {person_id: 0},  # Owner is initial follower with 0 contributions
                "articles": set()
            }
            # couple_sum not affected by account creation directly

    def _update_state_doa(self, params):
        state = self.network_state
        person_id, account_id = params["person_id"], params["account_id"]
        # Assuming JML checks passed
        if account_id in state["accounts"]:  # And owner is person_id
            del state["accounts"][account_id]
            state["couple_sum_dirty"] = True  # Might affect couple sum if official accounts were part of it

    def _update_state_ca(self, params):  # Contribute Article
        state = self.network_state
        person_id, account_id, article_id = params["person_id"], params["account_id"], params["article_id"]
        # Assuming JML checks passed
        if account_id in state["accounts"] and person_id in state["accounts"][account_id]["followers"]:
            acc_data = state["accounts"][account_id]
            acc_data["articles"].add(article_id)
            state["articles_map"][article_id] = person_id  # person_id is the contributor for this article
            acc_data["followers"][person_id] = acc_data["followers"].get(person_id, 0) + 1

            # Add article to received list of ALL followers of this account
            for follower_pid in acc_data["followers"].keys():
                if follower_pid in state["persons"]:
                    state["persons"][follower_pid]["articles_received_ids"].appendleft(article_id)
                    # JML for Person.getReceivedArticles() does not impose a strict limit on size of internal list
                    # The queryReceivedArticles() method returns only top 5.

    def _update_state_da(self, params):  # Delete Article
        state = self.network_state
        person_id, account_id, article_id = params["person_id"], params["account_id"], params["article_id"]
        # Assuming JML checks passed (person is owner, account has article)
        if account_id in state["accounts"] and article_id in state["accounts"][account_id]["articles"]:
            acc_data = state["accounts"][account_id]
            original_contributor_id = state["articles_map"].get(article_id)  # Get who originally contributed it

            acc_data["articles"].discard(article_id)
            if article_id in state["articles_map"]:
                del state["articles_map"][article_id]

            if original_contributor_id is not None and original_contributor_id in acc_data["followers"]:
                acc_data["followers"][original_contributor_id] -= 1
                if acc_data["followers"][original_contributor_id] < 0:  # Should not happen
                    acc_data["followers"][original_contributor_id] = 0

            # Remove from all followers' received lists
            for follower_pid in acc_data["followers"].keys():
                if follower_pid in state["persons"] and "articles_received_ids" in state["persons"][follower_pid]:
                    # Remove ALL occurrences, as JML implies it's gone
                    new_deque = deque()
                    for item in state["persons"][follower_pid]["articles_received_ids"]:
                        if item != article_id:
                            new_deque.append(item)
                    state["persons"][follower_pid]["articles_received_ids"] = new_deque

    def _update_state_foa(self, params):  # Follow Official Account
        state = self.network_state
        person_id, account_id = params["person_id"], params["account_id"]
        # Assuming JML checks passed
        if account_id in state["accounts"] and person_id in state["persons"]:
            state["accounts"]["account_id"].setdefault("followers", {})[person_id] = 0 # New follower, 0 contributions
            state["couple_sum_dirty"] = True  # Follow actions might affect couple sum if it considers official accounts

            # --- END OF HW10 STATE UPDATERS (placeholder) ---

            # --- HW11 New State Update Functions ---

    def _update_state_add_message_generic(self, internal_msg_params):
        """Generic helper for adding any message type to network_state.messages_map."""
        state = self.network_state
        msg_id = internal_msg_params["msg_id"]
        # Assumes JML pre-conditions for addMessage are met by the generator
        # (e.g., msg_id not exists, emojiId exists if EmojiMsg, articleId valid if ForwardMsg)

        # Construct the message object to store
        # This uses the simplified structure we defined.
        # If a more complex object simulation is needed, this would change.
        message_to_store = self._generate_message_object_structure(
            msg_id,
            internal_msg_params["type_val"],
            internal_msg_params["social_value"],
            internal_msg_params["p1_id"],
            internal_msg_params.get("p2_id"),  # Might be None
            internal_msg_params.get("tag_id"),  # Might be None
            internal_msg_params["msg_kind"],
            internal_msg_params.get("emojiId"),
            internal_msg_params.get("lucky_money"),
            internal_msg_params.get("articleId")
        )
        state["messages_map"][msg_id] = message_to_store
        state["all_message_ids_ever_used"].add(msg_id)

    def _update_state_sm(self, params):  # SendMessage
        state = self.network_state
        msg_id_to_send = params["id"]

        # Assume JML pre-conditions for sendMessage are met (msg exists, relation/tag valid)
        if msg_id_to_send not in state["messages_map"]: return  # Should not happen if JML met

        msg_obj = state["messages_map"][msg_id_to_send]
        sender_id = msg_obj["person1_id"]
        sender_data = state["persons"].get(sender_id)
        if not sender_data: return  # Should not happen

        # Increment sender's social value
        sender_data["socialValue"] += msg_obj["socialValue"]

        # Handle money for sender if RedEnvelope
        if msg_obj["msg_kind"] == "red_envelope":
            total_red_envelope_money = msg_obj["lucky_money"]
            money_deducted_from_sender = 0
            if msg_obj["type"] == 0:  # person-to-person
                money_deducted_from_sender = total_red_envelope_money
            elif msg_obj["type"] == 1:  # group
                tag_owner_id = sender_id  # Sender is the tag owner for group messages
                tag_id = msg_obj["tag_id"]
                tag_members_map = state["person_tags"].get((tag_owner_id, tag_id), {})
                num_tag_members = len(tag_members_map)
                if num_tag_members > 0:
                    money_per_member = total_red_envelope_money // num_tag_members
                    money_deducted_from_sender = money_per_member * num_tag_members
            sender_data["money"] -= money_deducted_from_sender

        # Distribute to receivers
        receivers_data_list = []  # List of (person_data_dict, money_to_add, article_to_add)

        if msg_obj["type"] == 0:  # Person-to-person
            receiver_id = msg_obj["person2_id"]
            if receiver_id and receiver_id in state["persons"]:
                receiver_data = state["persons"][receiver_id]
                money_to_add_p2p = msg_obj["lucky_money"] if msg_obj["msg_kind"] == "red_envelope" else 0
                article_to_add_p2p = msg_obj["articleId"] if msg_obj["msg_kind"] == "forward" else None
                receivers_data_list.append((receiver_data, money_to_add_p2p, article_to_add_p2p))

        elif msg_obj["type"] == 1:  # Group message
            tag_owner_id = sender_id  # In our model, sender is the tag owner
            tag_id = msg_obj["tag_id"]
            tag_key = (tag_owner_id, tag_id)
            members_in_tag_map = state["person_tags"].get(tag_key, {})

            money_per_member_group = 0
            if msg_obj["msg_kind"] == "red_envelope" and len(members_in_tag_map) > 0:
                money_per_member_group = msg_obj["lucky_money"] // len(members_in_tag_map)

            for member_id in members_in_tag_map.keys():
                if member_id in state["persons"]:
                    member_data = state["persons"][member_id]
                    article_to_add_group = msg_obj["articleId"] if msg_obj["msg_kind"] == "forward" else None
                    receivers_data_list.append((member_data, money_per_member_group, article_to_add_group))

        # Apply updates to receivers
        for receiver_data, money_to_add, article_to_add in receivers_data_list:
            receiver_data["socialValue"] += msg_obj["socialValue"]
            if money_to_add > 0:
                receiver_data["money"] += money_to_add

            # Add message to receiver's message list (Person.messages in JML)
            receiver_data["messages_received_obj"].appendleft(msg_obj)  # Add to front
            # JML for Person.getReceivedMessages (query) limits to 5, but internal list can be larger.

            if article_to_add is not None:  # Forward message
                receiver_data["articles_received_ids"].appendleft(article_to_add)
                # JML for Person.getReceivedArticles (query) limits to 5.

        # Handle Emoji Heat List
        if msg_obj["msg_kind"] == "emoji":
            emoji_id_sent = msg_obj["emojiId"]
            try:
                idx = state["emoji_id_list"].index(emoji_id_sent)
                state["emoji_heat_list"][idx] += 1
            except ValueError:
                pass  # Emoji not in list, should not happen if addMessage was correct

        # Remove message from network's list of active messages
        del state["messages_map"][msg_id_to_send]

    def _update_state_sei(self, params):  # StoreEmojiId
        state = self.network_state
        emoji_id_to_store = params["id"]
        # Assumes JML pre-condition (!containsEmojiId) is met
        if emoji_id_to_store not in state["emoji_id_list"]:
            state["emoji_id_list"].append(emoji_id_to_store)
            state["emoji_heat_list"].append(0)  # New emoji starts with 0 heat

    def _update_state_dce(self, params):  # DeleteColdEmoji
        state = self.network_state
        limit = params["limit"]

        new_emoji_id_list = []
        new_emoji_heat_list = []

        # Filter emoji lists
        for i in range(len(state["emoji_id_list"])):
            emoji_id = state["emoji_id_list"][i]
            heat = state["emoji_heat_list"][i]
            if heat >= limit:
                new_emoji_id_list.append(emoji_id)
                new_emoji_heat_list.append(heat)

        state["emoji_id_list"] = new_emoji_id_list
        state["emoji_heat_list"] = new_emoji_heat_list

        # Filter messages map
        # JML: ensures messages.length == (\num_of ... if EmojiMessage => containsEmojiId(updated_list)...)
        # This means remove EmojiMessages whose emojiId is no longer in the updated emoji_id_list.
        messages_to_delete_ids = []
        for msg_id, msg_obj in state["messages_map"].items():
            if msg_obj["msg_kind"] == "emoji":
                if msg_obj["emojiId"] not in state["emoji_id_list"]:
                    messages_to_delete_ids.append(msg_id)

        for msg_id_del in messages_to_delete_ids:
            if msg_id_del in state["messages_map"]:  # Check again before del
                del state["messages_map"][msg_id_del]

    _COMMAND_GENERATOR_METHODS = {
        "ap": _generate_ap, "ar": _generate_ar, "mr": _generate_mr,
        "at": _generate_at, "dt": _generate_dt, "att": _generate_att,
        "dft": _generate_dft, "qv": _generate_qv, "qci": _generate_qci,
        "qts": _generate_qts, "qtav": _generate_qtav, "qba": _generate_qba,
        "ln": _generate_ln,
        "coa": _generate_coa, "doa": _generate_doa, "ca": _generate_ca,
        "da": _generate_da, "foa": _generate_foa, "qsp": _generate_qsp,
        "qbc": _generate_qbc, "qra": _generate_qra, "qtvs": _generate_qtvs,
        "qcs": _generate_qcs,
        # HW11
        "am": _generate_add_ordinary_message,
        "sm": _generate_sm,
        "qsv": _generate_qsv,
        "qrm": _generate_qrm,
        "arem": _generate_add_red_envelope_message,
        "afm": _generate_add_forward_message,
        "aem": _generate_add_emoji_message,
        "sei": _generate_sei,
        "qp": _generate_qp,
        "dce": _generate_dce,
        "qm": _generate_qm,
    }

    _ALIAS_TO_UPDATE_METHOD_MAP = {
        "ap": _update_state_ap, "ar": _update_state_ar, "mr": _update_state_mr,
        "at": _update_state_at, "dt": _update_state_dt, "att": _update_state_att,
        "dft": _update_state_dft,
        "coa": _update_state_coa, "doa": _update_state_doa, "ca": _update_state_ca,
        "da": _update_state_da, "foa": _update_state_foa,
        # HW11
        "am": _update_state_add_message_generic,  # Uses internal_message_object_params
        "arem": _update_state_add_message_generic,
        "afm": _update_state_add_message_generic,
        "aem": _update_state_add_message_generic,
        "sm": _update_state_sm,
        "sei": _update_state_sei,
        "dce": _update_state_dce,
        # Queries and ln do not have separate update methods here
    }

    _STATE_UPDATE_METHODS = {
        INSTRUCTION_MAP.get(alias): method
        for alias, method in _ALIAS_TO_UPDATE_METHOD_MAP.items()
        if INSTRUCTION_MAP.get(alias) is not None
    }

    def _update_phase(self):  # Unchanged
        total_target = self.target_instructions
        instr_count = self.instructions_generated
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

    def generate_load_network(self):  # Unchanged from previous logic for ln
        if self.instructions_generated > 0: return 0
        result = self._generate_ln(target_key=None)
        if result and result[0] is not None:
            cmd_str, _, _ = result
            lines = cmd_str.strip().split('\n')
            self.generated_lines.extend(lines)
            # State update for ln is handled *within* _generate_ln
            # print(f"Generated load_network (first line: {lines[0]}). State updated IN GENERATOR.")
            return 1
        else:  # Fallback for ln
            self._initialize_state()
            n = 2;
            ids = [1, 2];
            names = ["p1", "p2"];
            ages = [20, 30]
            self.generated_lines.extend([f"ln {n}", " ".join(map(str, ids)), " ".join(names), " ".join(map(str, ages))])
            self._update_state_ap({"id": 1, "name": "p1", "age": 20})
            self._update_state_ap({"id": 2, "name": "p2", "age": 30})
            print("Warning: _generate_ln failed, generated minimal fallback ln.")
            return 1

    def _generate_arguments(self, cmd_alias, force_valid=False, force_exception_name=None):  # Logic largely unchanged
        target_key = None
        attempted_exception = False
        if force_valid:
            target_key = None
        elif force_exception_name:
            attempted_exception = True
            possible_keys = [k for k in EXCEPTION_NAME_TO_TARGET_KEY.get(force_exception_name, []) if k[0] == cmd_alias]
            if possible_keys:
                target_key = random.choice(possible_keys)
            else:
                return None
            self.exceptions_attempted.add(target_key)
        else:
            error_attempt_prob = 0.4
            if random.random() < error_attempt_prob:
                possible_exceptions_for_cmd = list(EXCEPTION_MAP.get(cmd_alias, set()))
                if possible_exceptions_for_cmd:
                    target_exc_name = random.choice(possible_exceptions_for_cmd)
                    possible_keys = [k for k in EXCEPTION_NAME_TO_TARGET_KEY.get(target_exc_name, []) if
                                     k[0] == cmd_alias]
                    if possible_keys:
                        target_key = random.choice(possible_keys)
                        attempted_exception = True
                        self.exceptions_attempted.add(target_key)
            if target_key is None: target_key = None

        generator_method = self._COMMAND_GENERATOR_METHODS.get(cmd_alias)
        if not generator_method:
            print(f"Error: No generator method found for '{cmd_alias}'", file=sys.stderr)
            return None
        result = generator_method(self, target_key=target_key)

        if result is None or result[0] is None: return None
        cmd_str, params, outcome = result

        if outcome == OUTCOME_NORMAL:
            self.commands_successfully_generated.add(cmd_alias)
            full_command_name = INSTRUCTION_MAP.get(cmd_alias)
            update_method = self._STATE_UPDATE_METHODS.get(full_command_name)
            if update_method:
                try:
                    # For message add commands, params already contains "internal_message_object_params"
                    # The update method will use that.
                    if cmd_alias in ["am", "arem", "afm", "aem"]:
                        update_method(self, params["internal_message_object_params"])
                    else:
                        update_method(self, params)
                except Exception as e:
                    print(f"CRITICAL ERROR during state update for {cmd_alias} (normal): {e}", file=sys.stderr)
                    print(f"Params: {params}", file=sys.stderr)
        elif outcome != OUTCOME_NORMAL:  # Exception generated
            expected_exc_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
            if outcome != expected_exc_name:
                pass  # print(f"Warning: Gen for {target_key} produced '{outcome}' vs map '{expected_exc_name}'", file=sys.stderr)
            if target_key: self.exceptions_attempted.add(target_key)
        return cmd_str

    def generate_instruction(self):  # Logic for command selection, weights, guarantees largely unchanged
        selected_cmd_alias = None
        force_valid = False
        force_exception_name = None
        coverage_attempt_prob = 0.15
        pending_success_cmds = list(COMMANDS - self.commands_successfully_generated)
        pending_exception_keys = list(self.all_exceptions_to_attempt - self.exceptions_attempted)
        action_taken = False

        if (pending_success_cmds or pending_exception_keys) and random.random() < coverage_attempt_prob:
            prioritize_success_prob = 0.6
            if pending_success_cmds and (not pending_exception_keys or random.random() < prioritize_success_prob):
                selected_cmd_alias = random.choice(pending_success_cmds)
                force_valid = True;
                action_taken = True
            elif pending_exception_keys:
                target_key = random.choice(pending_exception_keys)
                selected_cmd_alias, _ = target_key
                force_exception_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
                if force_exception_name:
                    action_taken = True
                else:
                    self.exceptions_attempted.add(target_key)

        if not action_taken:
            # Adjusted weights for HW11 commands might be needed here based on testing goals
            COMMAND_WEIGHTS = {
                "ap": 10, "ar": 10, "mr": 8, "at": 7, "dt": 4, "att": 7, "dft": 4,
                "qv": 5, "qci": 5, "qts": 2, "qtav": 5, "qtvs": 5, "qba": 5, "qcs": 3,
                "coa": 7, "doa": 4, "ca": 8, "da": 5, "foa": 6, "qsp": 7, "qbc": 4, "qra": 4,
                # HW11 weights - adjust these based on importance/complexity
                "am": 10, "sm": 12, "qsv": 5, "qrm": 5, "arem": 9, "afm": 9, "aem": 9,
                "sei": 6, "qp": 5, "dce": 3, "qm": 5,
            }
            temp_weights = defaultdict(float, COMMAND_WEIGHTS)
            state = self.network_state
            # Pruning logic (ensure it considers new state elements like messages_map, emoji_id_list)
            if not state["persons"]:
                runnable_cmds = ['ap']; cmd_weights = [1.0]
            else:
                # Pruning conditions for new commands:
                if not state["messages_map"]:  # No messages exist
                    for cmd in ['sm']: temp_weights[cmd] = 0
                if not state["emoji_id_list"]:  # No emojis stored
                    for cmd in ['aem', 'qp', 'dce']: temp_weights[
                        cmd] = 0  # aem needs stored emoji for normal, qp, dce operate on stored
                if not state["articles_map"]:  # No articles exist in network
                    for cmd in ['afm']: temp_weights[cmd] = 0  # afm needs article to forward

                # Existing pruning...
                num_persons = len(state["persons"])
                if num_persons < 2:
                    for cmd_p in ['ar', 'mr', 'qv', 'qci', 'att', 'dft', 'qsp', 'qtvs', 'qtav', 'qcs', 'qba']:
                        temp_weights[cmd_p] = 0
                if not state["relations"]:
                    for cmd_r in ['mr', 'qv', 'att', 'qba']: temp_weights[cmd_r] = 0
                if not any(p_data.get("tags") for p_data in state["persons"].values()):  # No person owns any tag
                    for cmd_t in ['dt', 'qtvs', 'qtav', 'att', 'dft', 'sm']:  # sm type 1 needs tag
                        if cmd_t == 'sm' and random.random() < 0.5:
                            pass  # Allow sm type 0
                        else:
                            temp_weights[cmd_t] = 0
                if not state["person_tags"]:  # No tag has any members
                    for cmd_tm in ['dft', 'qtav', 'qtvs']: temp_weights[cmd_tm] = 0
                if not state["accounts"]:
                    for cmd_acc in ['doa', 'ca', 'da', 'foa', 'qbc', 'qra']: temp_weights[cmd_acc] = 0
                if not state["articles_map"]:
                    if not any(acc_data.get("articles") for acc_data in state["accounts"].values()): temp_weights[
                        'da'] = 0
                if not any(acc_data.get("followers") for acc_data in state["accounts"].values()): temp_weights['ca'] = 0

                runnable_cmds_final = []
                cmd_weights_final = []
                for cmd_alias_iter in COMMANDS:  # Iterate over all possible command aliases
                    w = temp_weights.get(cmd_alias_iter, 0)
                    if w > 0:
                        runnable_cmds_final.append(cmd_alias_iter)
                        cmd_weights_final.append(w)
                if not runnable_cmds_final:
                    selected_cmd_alias = 'ap'
                elif sum(cmd_weights_final) > 0:
                    selected_cmd_alias = random.choices(runnable_cmds_final, cmd_weights_final, k=1)[0]
                else:
                    selected_cmd_alias = random.choice(runnable_cmds_final) if runnable_cmds_final else 'ap'

        if selected_cmd_alias:
            return self._generate_arguments(selected_cmd_alias, force_valid=force_valid,
                                            force_exception_name=force_exception_name)
        else:  # Should ideally not happen if 'ap' is ultimate fallback
            return self._generate_arguments('ap', force_valid=True)

    def generate(self):  # Main loop unchanged structurally
        self._initialize_state()
        self.instructions_generated += self.generate_load_network()
        self._update_phase()

        attempts = 0
        max_attempts_factor = 25
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
            if stuck_counter >= max_stuck_count: break

        # Final Guarantee Attempt (logic unchanged)
        missing_success_cmds = list(COMMANDS - self.commands_successfully_generated)
        final_success_attempts = 0;
        max_final_success_attempts = len(missing_success_cmds) * 25
        while missing_success_cmds and final_success_attempts < max_final_success_attempts and len(
                self.generated_lines) < self.max_instr_limit:
            final_success_attempts += 1;
            cmd_to_add = missing_success_cmds[0]
            instr_str = self._generate_arguments(cmd_to_add, force_valid=True)
            if instr_str:
                self.generated_lines.append(instr_str)
                if cmd_to_add in self.commands_successfully_generated:
                    missing_success_cmds.pop(0)
                else:
                    missing_success_cmds.append(missing_success_cmds.pop(0))
            else:
                missing_success_cmds.append(missing_success_cmds.pop(0))

        missing_exception_keys = list(self.all_exceptions_to_attempt - self.exceptions_attempted)
        final_exception_attempts = 0;
        max_final_exception_attempts = len(missing_exception_keys) * 15
        while missing_exception_keys and final_exception_attempts < max_final_exception_attempts and len(
                self.generated_lines) < self.max_instr_limit:
            final_exception_attempts += 1;
            target_key = missing_exception_keys[0]
            cmd_alias, _ = target_key;
            exc_name = GENERATOR_TARGET_OUTCOME_MAP.get(target_key)
            if exc_name:
                instr_str = self._generate_arguments(cmd_alias, force_exception_name=exc_name)
                if target_key in self.exceptions_attempted:
                    missing_exception_keys.pop(0)
                    if instr_str: self.generated_lines.append(instr_str)
                else:
                    missing_exception_keys.append(missing_exception_keys.pop(0))
            else:
                self.exceptions_attempted.add(target_key);
                missing_exception_keys.pop(0)

        # Final Report (unchanged)
        # ...
        final_lines = [str(line) for line in self.generated_lines if line is not None]
        return final_lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate HW11 Test Data.")
    parser.add_argument("-m", "--mode", choices=['P', 'M'], default='P', help="Test mode")
    parser.add_argument("-n", "--num_instructions", type=int, default=1000,
                        help="Target logical instructions")  # Reduced default for quicker test
    parser.add_argument("-o", "--output", type=str, default="generated_hw11_data.txt", help="Output file")
    args = parser.parse_args()

    start_time = time.time()
    generator = DataGenerator(mode=args.mode, num_logical_instructions=args.num_instructions)
    generated_instruction_lines = generator.generate()
    end_time = time.time()
    print(f"\nGeneration took {end_time - start_time:.2f} seconds.")

    try:
        with open(args.output, "w", encoding="utf-8") as f:
            current_line_count = 0
            for line_content in generated_instruction_lines:
                lines_to_write = str(line_content).split('\n')
                for single_line in lines_to_write:
                    if current_line_count < generator.max_instr_limit:
                        f.write(single_line + "\n")
                        current_line_count += 1
                    else:
                        break
                if current_line_count >= generator.max_instr_limit: break
        print(f"Successfully wrote {current_line_count} lines to {args.output}")
        # Final report print statements from original generator
        missing_success_final = list(COMMANDS - generator.commands_successfully_generated)
        missing_exceptions_final = list(generator.all_exceptions_to_attempt - generator.exceptions_attempted)
        if missing_success_final: print(
            f"Warning: Could not guarantee successful generation for: {sorted(list(missing_success_final))}")
        if missing_exceptions_final: print(
            f"Warning: Could not guarantee exception attempt for: {sorted([f'{tk[0]}-{GENERATOR_TARGET_OUTCOME_MAP.get(tk)}' for tk in missing_exceptions_final])}")
        print(
            f"Generator finished: generated {len(generator.generated_lines)} raw lines (target logical: {generator.target_instructions}).")
        print(
            f"Successfully generated commands guaranteed: {len(generator.commands_successfully_generated)}/{len(COMMANDS)}")
        print(
            f"Attempted exceptions guaranteed: {len(generator.exceptions_attempted)}/{len(generator.all_exceptions_to_attempt)}")

    except IOError as e:
        print(f"Error writing to output file {args.output}: {e}")