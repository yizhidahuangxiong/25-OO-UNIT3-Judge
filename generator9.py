import random
import string
import os
import argparse
import enum

# --- Constants ---
MAX_STRING_LEN = 10
MIN_AGE = 1
MAX_AGE = 200
MIN_VALUE = 1
MAX_VALUE = 200
MIN_M_VAL = -200
MAX_M_VAL = 200
PUBLIC_MAX_INSTRUCTIONS = 10000
PUBLIC_MAX_N_LOAD = 300
MUTUAL_MAX_INSTRUCTIONS = 3000
MUTUAL_MAX_N_LOAD = 100

COMMANDS = {
    "ap": "add_person", "ar": "add_relation", "qv": "query_value",
    "qci": "query_circle", "qts": "query_triple_sum", "at": "add_tag",
    "att": "add_to_tag", "dft": "del_from_tag", "qtav": "query_tag_age_var",
    "mr": "modify_relation", "qba": "query_best_acquaintance",
    "dt": "del_tag", "ln": "load_network",
}
SIMPLE_COMMANDS = ["qts"]

# --- Generation Phases ---
class GenPhase(enum.Enum):
    INITIAL_LOAD = 0
    BUILD_NETWORK = 1
    RANDOM_MIX = 2
    QTS_STRESS = 3

# --- Helper ---
def random_string(length=MAX_STRING_LEN):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

# --- Generator Class ---
class DataGenerator:
    def __init__(self, mode='P', num_logical_instructions=100):
        self.mode = mode.upper()
        self.target_instructions = num_logical_instructions
        if self.mode == 'P':
            self.max_instr_limit = PUBLIC_MAX_INSTRUCTIONS;
            self.max_n_load = PUBLIC_MAX_N_LOAD
        else: # Mutual Mode
            self.max_instr_limit = MUTUAL_MAX_INSTRUCTIONS;
            self.max_n_load = MUTUAL_MAX_N_LOAD
        self.target_instructions = min(self.target_instructions, self.max_instr_limit)
        self.person_ids = set();
        self.relations = set();
        self.person_tags = {} # person_id -> set(tag_id)
        self.tag_persons = {}; # tag_id -> set(person_id)
        self.next_person_id_counter = 0;
        self.next_tag_id_counter = 0
        self.instructions_generated = 0
        self.current_phase = GenPhase.INITIAL_LOAD
        self.phase_instruction_count = 0
        self.tag_sizes = {} # tag_id -> size

    # --- State Helpers ---
    def _get_new_person_id(self):
        self.next_person_id_counter += random.randint(1, 5); return self.next_person_id_counter

    def _get_new_tag_id(self):
        self.next_tag_id_counter += random.randint(1, 3); return self.next_tag_id_counter

    def _get_existing_person_id(self, allow_none=False):
        if not self.person_ids: return None if allow_none else -999
        try:
            return random.choice(list(self.person_ids))
        except IndexError: return None if allow_none else -999

    def _get_two_distinct_person_ids(self, allow_none=False):
        if len(self.person_ids) < 2: return (None, None) if allow_none else (-999, -998)
        try:
            return tuple(random.sample(list(self.person_ids), 2))
        except ValueError: return (None, None) if allow_none else (-999, -998)

    def _get_existing_tag_id_for_person(self, person_id, allow_none=False):
        if person_id not in self.person_tags or not self.person_tags[person_id]: return None if allow_none else -9999
        try:
            return random.choice(list(self.person_tags[person_id]))
        except IndexError: return None if allow_none else -9999

    def _get_relation(self, allow_none=False):
        if not self.relations: return (None, None) if allow_none else (-999, -998)
        try:
            id1, id2 = random.choice(list(self.relations))
            return (id1, id2) if random.random() < 0.5 else (id2, id1)
        except IndexError: return (None, None) if allow_none else (-999, -998)

    # --- Phase Management ---
    def _update_phase(self):
        total_target = self.target_instructions
        instr_count = self.instructions_generated
        build_end = total_target * 0.35
        mix_end = total_target * 0.70
        if self.current_phase == GenPhase.INITIAL_LOAD:
            self.current_phase = GenPhase.BUILD_NETWORK
            self.phase_instruction_count = 0
        elif self.current_phase == GenPhase.BUILD_NETWORK and instr_count >= build_end:
            self.current_phase = GenPhase.RANDOM_MIX
            self.phase_instruction_count = 0
        elif self.current_phase == GenPhase.RANDOM_MIX and instr_count >= mix_end:
            self.current_phase = GenPhase.QTS_STRESS
            self.phase_instruction_count = 0

    # --- Instruction Generation ---
    def generate_load_network(self):
        if self.instructions_generated > 0 or random.random() < 0.1: return []
        max_possible_n = min(self.max_n_load, 80, self.target_instructions // 3)
        if max_possible_n <= 1: return []
        n = random.randint(10, max_possible_n)
        start_id = self.next_person_id_counter + 100;
        ids = [start_id + i for i in range(n)]
        names = [str(id_val) for id_val in ids];
        ages = [id_val % MAX_AGE + MIN_AGE for id_val in ids]
        lines = [f"ln {n}", " ".join(map(str, ids)), " ".join(names), " ".join(map(str, ages))]
        person_ids_added = set()
        relations_added = set()
        person_tags_added = {}
        for pid in ids:
            person_ids_added.add(pid)
            person_tags_added[pid] = set()
        self.next_person_id_counter = max(self.next_person_id_counter, ids[-1] if ids else 0)
        relation_lines = []
        for i in range(1, n):
            row_values = []
            for j in range(i):
                if random.random() < 0.35:
                    value = random.randint(MIN_VALUE, MAX_VALUE)
                    row_values.append(str(value))
                    relations_added.add(tuple(sorted((ids[i], ids[j]))))
                else: row_values.append("0")
            relation_lines.append(" ".join(row_values))
        lines.extend(relation_lines)
        self.person_ids.update(person_ids_added)
        self.relations.update(relations_added)
        self.person_tags.update(person_tags_added)
        print(f"Generated load_network with {n} people ({len(lines)} lines).")
        return lines

    def _generate_specific_value_mr(self, p1, p2):
        return random.randint(MIN_M_VAL, MAX_M_VAL)

    # --- _generate_arguments (Prioritizes Valid Cases) ---
    def _generate_arguments(self, cmd):
        force_error_prob = 0.15
        attempt_error_first = random.random() < force_error_prob

        # --- Specific Command Logic (Prioritizing Valid) ---
        if cmd == "ap":
            pid_new = self._get_new_person_id()
            name = str(pid_new)
            age = pid_new % MAX_AGE + MIN_AGE
            valid_instr = f"ap {pid_new} {name} {age}"
            can_generate_valid = True # Always possible to generate new AP

            if attempt_error_first and self.person_ids:
                pid_err = self._get_existing_person_id(allow_none=True)
                if pid_err is not None: return f"ap {pid_err} {name} {age}"
                else: # Fallback if no existing person
                    self.person_ids.add(pid_new); self.person_tags[pid_new] = set()
                    return valid_instr
            else:
                self.person_ids.add(pid_new); self.person_tags[pid_new] = set()
                return valid_instr

        elif cmd == "ar":
            p1_valid, p2_valid = self._get_two_distinct_person_ids(allow_none=True)
            rel_valid = tuple(sorted((p1_valid, p2_valid))) if p1_valid is not None else None
            can_generate_valid = (p1_valid is not None and rel_valid not in self.relations)
            value = random.randint(MIN_VALUE, MAX_VALUE)

            if can_generate_valid and not attempt_error_first:
                self.relations.add(rel_valid)
                return f"ar {p1_valid} {p2_valid} {value}"
            else: # Attempt error or handle inability to generate valid
                p1_err, p2_err = self._get_two_distinct_person_ids(allow_none=True)
                if p1_err is None:
                     if can_generate_valid:
                         self.relations.add(rel_valid)
                         return f"ar {p1_valid} {p2_valid} {value}"
                     else: return None
                rel_err = tuple(sorted((p1_err, p2_err)))
                if rel_err in self.relations and random.random() < 0.5: return f"ar {p1_err} {p2_err} {value}" # Try ER
                else: # Try PINF
                    p1_force_err = -999 if random.random() < 0.5 else p1_err
                    p2_force_err = -998 if p1_force_err != -999 else p2_err
                    if p1_force_err == p1_err and p2_force_err == p2_err : p1_force_err = -999
                    return f"ar {p1_force_err} {p2_force_err} {value}"
                # Fallback to valid if error gen failed (logic implies error was attempted first)
                if can_generate_valid:
                    self.relations.add(rel_valid)
                    return f"ar {p1_valid} {p2_valid} {value}"
                else: return None

        elif cmd == "mr":
            p1_valid, p2_valid = self._get_relation(allow_none=True)
            can_generate_valid = p1_valid is not None
            value = self._generate_specific_value_mr(p1_valid if p1_valid else -1, p2_valid if p2_valid else -1)

            if can_generate_valid and not attempt_error_first:
                return f"mr {p1_valid} {p2_valid} {value}"
            else: # Attempt error or handle inability to generate valid
                error_type = random.choice(["PINF1", "PINF2", "EPI", "RNF"])
                p1_err, p2_err = self._get_two_distinct_person_ids(allow_none=True)
                if p1_err is None:
                     if can_generate_valid: return f"mr {p1_valid} {p2_valid} {value}"
                     else: return None
                if error_type == "PINF1": return f"mr {-999} {p2_err} {value}"
                elif error_type == "PINF2": return f"mr {p1_err} {-998} {value}"
                elif error_type == "EPI":
                     existing_p = self._get_existing_person_id(allow_none=True)
                     if existing_p is not None: return f"mr {existing_p} {existing_p} {value}"
                elif error_type == "RNF":
                     for _ in range(5):
                         p1_rnf, p2_rnf = self._get_two_distinct_person_ids(allow_none=True)
                         if p1_rnf is not None and tuple(sorted((p1_rnf, p2_rnf))) not in self.relations:
                             return f"mr {p1_rnf} {p2_rnf} {value}"
                # Fallback if error generation failed
                if can_generate_valid: return f"mr {p1_valid} {p2_valid} {value}"
                else: return None

        elif cmd == "at":
            person_id_valid = self._get_existing_person_id(allow_none=True)
            tag_id_new = self._get_new_tag_id()
            can_generate_valid = person_id_valid is not None and tag_id_new not in self.person_tags.get(person_id_valid, set())

            if can_generate_valid and not attempt_error_first:
                if person_id_valid not in self.person_tags: self.person_tags[person_id_valid] = set()
                self.person_tags[person_id_valid].add(tag_id_new)
                self.tag_sizes[tag_id_new] = 0
                return f"at {person_id_valid} {tag_id_new}"
            else: # Attempt error
                error_type = random.choice(["PINF", "ETI"])
                if error_type == "PINF": return f"at {-999} {tag_id_new}"
                elif error_type == "ETI":
                    tag_id_existing = self._get_existing_tag_id_for_person(person_id_valid, allow_none=True)
                    if tag_id_existing is not None: return f"at {person_id_valid} {tag_id_existing}"
                # Fallback
                if can_generate_valid:
                    if person_id_valid not in self.person_tags: self.person_tags[person_id_valid] = set()
                    self.person_tags[person_id_valid].add(tag_id_new)
                    self.tag_sizes[tag_id_new] = 0
                    return f"at {person_id_valid} {tag_id_new}"
                else: return None

        elif cmd == "att":
            # Try find valid components first
            p1_valid, p2_valid = None, None
            tag_id_valid = None
            valid_options = []
            for p2_cand in self.person_ids:
                 tags_for_p2 = self.person_tags.get(p2_cand, set())
                 if not tags_for_p2: continue
                 tag_id_cand = random.choice(list(tags_for_p2)) # Pick a tag p2 has
                 # Find p1 candidates related to p2 but not in tag
                 for p1_cand in self.person_ids:
                      if p1_cand != p2_cand and \
                         tuple(sorted((p1_cand, p2_cand))) in self.relations and \
                         p1_cand not in self.tag_persons.get(tag_id_cand, set()):
                           valid_options.append((p1_cand, p2_cand, tag_id_cand))
                           break # Found one p1 for this p2/tag, move on
            if valid_options:
                p1_valid, p2_valid, tag_id_valid = random.choice(valid_options)
            can_generate_valid = p1_valid is not None

            if can_generate_valid and not attempt_error_first:
                 current_size = self.tag_sizes.get(tag_id_valid, 0)
                 if current_size <= 999:
                      if tag_id_valid not in self.tag_persons: self.tag_persons[tag_id_valid] = set()
                      self.tag_persons[tag_id_valid].add(p1_valid)
                      self.tag_sizes[tag_id_valid] = current_size + 1
                 return f"att {p1_valid} {p2_valid} {tag_id_valid}"
            else: # Attempt error
                 p1_err, p2_err = self._get_two_distinct_person_ids(allow_none=True)
                 if p1_err is None: # Need people for error
                     if can_generate_valid: # Fallback
                         current_size = self.tag_sizes.get(tag_id_valid, 0)
                         if current_size <= 999:
                             if tag_id_valid not in self.tag_persons: self.tag_persons[tag_id_valid] = set()
                             self.tag_persons[tag_id_valid].add(p1_valid)
                             self.tag_sizes[tag_id_valid] = current_size + 1
                         return f"att {p1_valid} {p2_valid} {tag_id_valid}"
                     else: return None

                 tag_id_for_p2_err = self._get_existing_tag_id_for_person(p2_err, allow_none=True)
                 rel_exists_err = tuple(sorted((p1_err, p2_err))) in self.relations
                 p1_in_tag_err = tag_id_for_p2_err is not None and p1_err in self.tag_persons.get(tag_id_for_p2_err, set())
                 possible_errors = ["PINF1", "PINF2", "EPI_SAME", "RNF", "TINF", "EPI_TAG"]
                 error_type = random.choice(possible_errors)
                 tid_arg = tag_id_for_p2_err if tag_id_for_p2_err else -9999
                 if error_type == "PINF1": return f"att {-999} {p2_err} {tid_arg}"
                 elif error_type == "PINF2": return f"att {p1_err} {-998} {tid_arg}"
                 elif error_type == "EPI_SAME": return f"att {p1_err} {p1_err} {tid_arg}"
                 elif error_type == "RNF" and not rel_exists_err: return f"att {p1_err} {p2_err} {tid_arg}"
                 elif error_type == "TINF" and tag_id_for_p2_err is None: return f"att {p1_err} {p2_err} {-9999}"
                 elif error_type == "EPI_TAG" and p1_in_tag_err: return f"att {p1_err} {p2_err} {tag_id_for_p2_err}"
                 # Fallback if error generation failed
                 if can_generate_valid:
                     current_size = self.tag_sizes.get(tag_id_valid, 0)
                     if current_size <= 999:
                         if tag_id_valid not in self.tag_persons: self.tag_persons[tag_id_valid] = set()
                         self.tag_persons[tag_id_valid].add(p1_valid)
                         self.tag_sizes[tag_id_valid] = current_size + 1
                     return f"att {p1_valid} {p2_valid} {tag_id_valid}"
                 else: return None

        elif cmd == "dft":
            # Try find valid components
            valid_removals = []
            for pid2, tags in self.person_tags.items():
                 for tid in tags:
                     for pid1 in self.tag_persons.get(tid, set()):
                         if pid1 != pid2: valid_removals.append((pid1, pid2, tid))
            can_generate_valid = bool(valid_removals)
            p1_valid, p2_valid, tag_id_valid = random.choice(valid_removals) if can_generate_valid else (None, None, None)

            if can_generate_valid and not attempt_error_first:
                if tag_id_valid in self.tag_persons:
                    self.tag_persons[tag_id_valid].discard(p1_valid)
                    self.tag_sizes[tag_id_valid] = self.tag_sizes.get(tag_id_valid, 1) - 1
                return f"dft {p1_valid} {p2_valid} {tag_id_valid}"
            else: # Attempt error
                p1_err, p2_err = self._get_two_distinct_person_ids(allow_none=True)
                if p1_err is None:
                     if can_generate_valid: # Fallback
                         if tag_id_valid in self.tag_persons:
                             self.tag_persons[tag_id_valid].discard(p1_valid)
                             self.tag_sizes[tag_id_valid] = self.tag_sizes.get(tag_id_valid, 1) - 1
                         return f"dft {p1_valid} {p2_valid} {tag_id_valid}"
                     else: return None
                tid_err = self._get_existing_tag_id_for_person(p2_err, allow_none=True)
                possible_errors = ["PINF1", "PINF2", "TINF", "PINF_TAG"]
                error_type = random.choice(possible_errors)
                tid_arg = tid_err if tid_err else -9999
                if error_type == "PINF1": return f"dft {-999} {p2_err} {tid_arg}"
                elif error_type == "PINF2": return f"dft {p1_err} {-998} {tid_arg}"
                elif error_type == "TINF": return f"dft {p1_err} {p2_err} {-9999}"
                elif error_type == "PINF_TAG" and tid_err is not None and p1_err not in self.tag_persons.get(tid_err, set()):
                     return f"dft {p1_err} {p2_err} {tid_err}"
                # Fallback
                if can_generate_valid:
                    if tag_id_valid in self.tag_persons:
                        self.tag_persons[tag_id_valid].discard(p1_valid)
                        self.tag_sizes[tag_id_valid] = self.tag_sizes.get(tag_id_valid, 1) - 1
                    return f"dft {p1_valid} {p2_valid} {tag_id_valid}"
                else: return None

        elif cmd == "dt":
            # Try find valid components
            person_id_valid = None
            tag_id_valid = None
            valid_options = []
            for pid, tags in self.person_tags.items():
                if tags: valid_options.append((pid, random.choice(list(tags))))
            if valid_options:
                person_id_valid, tag_id_valid = random.choice(valid_options)
            can_generate_valid = person_id_valid is not None

            if can_generate_valid and not attempt_error_first:
                if person_id_valid in self.person_tags:
                    self.person_tags[person_id_valid].discard(tag_id_valid)
                return f"dt {person_id_valid} {tag_id_valid}"
            else: # Attempt error
                error_type = random.choice(["PINF", "TINF"])
                p_err = self._get_existing_person_id(allow_none=True)
                if p_err is None: # Need person for TINF error
                    if can_generate_valid: # Fallback
                         if person_id_valid in self.person_tags:
                             self.person_tags[person_id_valid].discard(tag_id_valid)
                         return f"dt {person_id_valid} {tag_id_valid}"
                    else: return None
                if error_type == "PINF": return f"dt {-999} {tag_id_valid if tag_id_valid else -9999}"
                elif error_type == "TINF":
                    non_person_tags = list(set(self.tag_persons.keys()) - self.person_tags.get(p_err, set()))
                    tid_err = random.choice(non_person_tags) if non_person_tags else -9999
                    return f"dt {p_err} {tid_err}"
                # Fallback
                if can_generate_valid:
                    if person_id_valid in self.person_tags:
                        self.person_tags[person_id_valid].discard(tag_id_valid)
                    return f"dt {person_id_valid} {tag_id_valid}"
                else: return None

        elif cmd == "qv":
            p1_valid, p2_valid = self._get_relation(allow_none=True)
            can_generate_valid = p1_valid is not None

            if can_generate_valid and not attempt_error_first:
                return f"qv {p1_valid} {p2_valid}"
            else: # Attempt error
                error_type = random.choice(["PINF", "RNF"])
                p1_err, p2_err = self._get_two_distinct_person_ids(allow_none=True)
                if p1_err is None:
                     if can_generate_valid: return f"qv {p1_valid} {p2_valid}"
                     else: return None
                if error_type == "PINF":
                    p1_f = -999 if random.random() < 0.5 else p1_err
                    p2_f = -998 if p1_f != -999 else p2_err
                    if p1_f == p1_err and p2_f == p2_err: p1_f = -999
                    return f"qv {p1_f} {p2_f}"
                elif error_type == "RNF":
                    if tuple(sorted((p1_err, p2_err))) not in self.relations:
                        return f"qv {p1_err} {p2_err}"
                # Fallback
                if can_generate_valid: return f"qv {p1_valid} {p2_valid}"
                else: return None

        elif cmd == "qci":
             p1_valid, p2_valid = self._get_two_distinct_person_ids(allow_none=True)
             can_generate_valid = p1_valid is not None

             if can_generate_valid and not attempt_error_first:
                 return f"qci {p1_valid} {p2_valid}"
             else: # Attempt PINF error
                 if p1_valid is None: return None
                 if random.random() < 0.5: return f"qci {-999} {p2_valid}"
                 else: return f"qci {p1_valid} {-998}"
                 # Fallback not simple here if error chosen

        elif cmd == "qba":
            person_id_valid = self._get_existing_person_id(allow_none=True)
            can_generate_valid = person_id_valid is not None
            # Valid qba can still trigger ANF if no relations exist for person_id_valid
            has_acquaintance = can_generate_valid and any(person_id_valid in rel for rel in self.relations)

            if can_generate_valid and not attempt_error_first:
                 return f"qba {person_id_valid}"
            else: # Attempt error
                error_type = random.choice(["PINF", "ANF_FORCE"])
                if error_type == "PINF": return f"qba {-999}"
                elif error_type == "ANF_FORCE":
                    # Try find person *without* acquaintances to force ANF error case
                    candidates = list(self.person_ids)
                    random.shuffle(candidates)
                    p_anf = None
                    for pid_cand in candidates:
                        if not any(pid_cand in rel for rel in self.relations):
                            p_anf = pid_cand; break
                    if p_anf is not None: return f"qba {p_anf}"
                # Fallback
                if can_generate_valid: return f"qba {person_id_valid}"
                else: return None

        elif cmd == "qtav": # Logic already implemented above with prioritize-valid
            person_id_valid = None
            tag_id_valid = None
            valid_pairs = []
            for pid, tags in self.person_tags.items():
                if tags: valid_pairs.append((pid, random.choice(list(tags))))
            if valid_pairs: person_id_valid, tag_id_valid = random.choice(valid_pairs)
            can_generate_valid = person_id_valid is not None

            if can_generate_valid and not attempt_error_first:
                return f"qtav {person_id_valid} {tag_id_valid}"
            else: # Attempt error
                error_type = random.choice(["PINF", "TINF"])
                p_err = self._get_existing_person_id(allow_none=True)
                t_err = self._get_new_tag_id()
                if error_type == "PINF":
                    t_arg = tag_id_valid if tag_id_valid is not None else -9999
                    return f"qtav {-999} {t_arg}"
                elif error_type == "TINF" and p_err is not None:
                     if t_err not in self.person_tags.get(p_err, set()):
                          return f"qtav {p_err} {t_err}"
                # Fallback
                if can_generate_valid: return f"qtav {person_id_valid} {tag_id_valid}"
                else: return None

        elif cmd == "qts":
            return "qts" # Always valid if chosen

        # --- Fallback ---
        print(f"Warning: Reached end of _generate_arguments for cmd '{cmd}' without generating.")
        return None


    # --- generate_instruction (Uses the updated _generate_arguments) ---
    def generate_instruction(self):
        possible_cmds = list(COMMANDS.keys())
        if 'ln' in possible_cmds: possible_cmds.remove('ln')
        weights = {}

        current_phase = self.current_phase
        if self.instructions_generated == 0 and current_phase == GenPhase.INITIAL_LOAD:
            selected_cmd = 'ap'
            return self._generate_arguments(selected_cmd)

        if current_phase == GenPhase.BUILD_NETWORK:
            weights['ap'] = 5.0
            weights['ar'] = 4.0
            for cmd in possible_cmds:
                if cmd not in ['ap', 'ar']: weights[cmd] = 0.1
            weights['qts'] = 0
        elif current_phase == GenPhase.RANDOM_MIX:
            for cmd in possible_cmds: weights[cmd] = 1.0
        elif current_phase == GenPhase.QTS_STRESS:
            weights['qts'] = 8.0
            weights['ar'] = 2.0
            for cmd in possible_cmds:
                if cmd not in ['qts', 'ar']: weights[cmd] = 0.1
            weights['ap'] = 0.3

        if not self.person_ids:
            runnable_cmds = ['ap']; cmd_weights = [1.0]
        else:
            temp_weights = weights.copy()
            if len(self.person_ids) < 2:
                for cmd in ['ar', 'mr', 'qv', 'qci', 'att', 'dft']:
                     if cmd in temp_weights: temp_weights[cmd] = 0
            if not self.relations:
                 if 'mr' in temp_weights: temp_weights['mr'] = 0;
            runnable_cmds = []; cmd_weights = []
            for cmd in possible_cmds:
                w = temp_weights.get(cmd, 0)
                if w > 0: runnable_cmds.append(cmd); cmd_weights.append(w)

        if not runnable_cmds: selected_cmd = 'ap'
        else:
            if not any(w > 0 for w in cmd_weights): selected_cmd = 'ap'
            else: selected_cmd = random.choices(runnable_cmds, cmd_weights, k=1)[0]
        return self._generate_arguments(selected_cmd)

    # --- generate method ---
    def generate(self):
        instruction_lines = []
        self.instructions_generated = 0
        self.current_phase = GenPhase.INITIAL_LOAD
        self.phase_instruction_count = 0

        ln_lines = self.generate_load_network()
        if ln_lines:
            instruction_lines.extend(ln_lines)
            self.instructions_generated += 1
        self._update_phase() # Update phase after potential ln

        attempts = 0
        max_attempts = self.target_instructions * 8 + 200 # Adjusted max attempts

        while self.instructions_generated < self.target_instructions and attempts < max_attempts:
            instr_str = self.generate_instruction()
            if instr_str:
                instruction_lines.append(instr_str)
                self.instructions_generated += 1
                self.phase_instruction_count += 1
                self._update_phase() # Update phase after generating the instruction
            #else:
                # Optional: print if instruction generation failed
                #print(f"DEBUG: generate_instruction returned None at attempt {attempts}")

            attempts += 1


        if attempts >= max_attempts:
            print(f"Warning: Generator hit max attempts ({max_attempts}).")
        print(f"Generator finished: generated {self.instructions_generated} logical instructions ({len(instruction_lines)} lines). Final Phase: {self.current_phase}")
        return instruction_lines

# --- Main execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for Spec1 Network (Prioritize Valid).")
    parser.add_argument("-m", "--mode", choices=['P', 'M'], default='P', help="Test mode (P=Public, M=Mutual)")
    parser.add_argument("-n", "--num_instructions", type=int, default=100, help="Target number of LOGICAL instructions")
    parser.add_argument("-o", "--output", type=str, default="generated_data_valid_prio.txt", help="Output file name") # Updated default name
    args = parser.parse_args()

    generator = DataGenerator(mode=args.mode, num_logical_instructions=args.num_instructions)
    generated_instruction_lines = generator.generate()

    try:
        with open(args.output, "w", encoding="utf-8") as f:
            for line in generated_instruction_lines:
                f.write(line + "\n")
        print(f"Successfully wrote {len(generated_instruction_lines)} lines to {args.output}")
    except IOError as e:
        print(f"Error writing to output file {args.output}: {e}")