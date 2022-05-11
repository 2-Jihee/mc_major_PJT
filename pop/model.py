from pandas import isna

seoul_2020_12 = {
    0: {'M': 23202, 'F': 21961},
    1: {'M': 26272, 'F': 24204},
    2: {'M': 27551, 'F': 25534},
    3: {'M': 29263, 'F': 28038},
    4: {'M': 32873, 'F': 31472},
    5: {'M': 35231, 'F': 33514},
    6: {'M': 34745, 'F': 33231},
    7: {'M': 35008, 'F': 33122},
    8: {'M': 38624, 'F': 36629},
    9: {'M': 37118, 'F': 35374},
    10: {'M': 37722, 'F': 35815},
    11: {'M': 35728, 'F': 34110},
    12: {'M': 38475, 'F': 36260},
    13: {'M': 40704, 'F': 38472},
    14: {'M': 37778, 'F': 35327},
    15: {'M': 36809, 'F': 34364},
    16: {'M': 40559, 'F': 38170},
    17: {'M': 41890, 'F': 39505},
    18: {'M': 42059, 'F': 39873},
    19: {'M': 47936, 'F': 46936},
    20: {'M': 56061, 'F': 57323},
    21: {'M': 53898, 'F': 58438},
    22: {'M': 58294, 'F': 63424},
    23: {'M': 63413, 'F': 71587},
    24: {'M': 67447, 'F': 76269},
    25: {'M': 74078, 'F': 81060},
    26: {'M': 79830, 'F': 83840},
    27: {'M': 82196, 'F': 84142},
    28: {'M': 84946, 'F': 86746},
    29: {'M': 82914, 'F': 84782},
    30: {'M': 76371, 'F': 75424},
    31: {'M': 73168, 'F': 73712},
    32: {'M': 72712, 'F': 72200},
    33: {'M': 68458, 'F': 70412},
    34: {'M': 69033, 'F': 68454},
    35: {'M': 69548, 'F': 69407},
    36: {'M': 68190, 'F': 68069},
    37: {'M': 74818, 'F': 74083},
    38: {'M': 78505, 'F': 78992},
    39: {'M': 79575, 'F': 78502},
    40: {'M': 77488, 'F': 77753},
    41: {'M': 74846, 'F': 77753},
    42: {'M': 65298, 'F': 64384},
    43: {'M': 68369, 'F': 72073},
    44: {'M': 68778, 'F': 69452},
    45: {'M': 69342, 'F': 71764},
    46: {'M': 76365, 'F': 76524},
    47: {'M': 77451, 'F': 81184},
    48: {'M': 80342, 'F': 81217},
    49: {'M': 84709, 'F': 85587},
    50: {'M': 81289, 'F': 83186},
    51: {'M': 79949, 'F': 82015},
    52: {'M': 77305, 'F': 80595},
    53: {'M': 70591, 'F': 76788},
    54: {'M': 68942, 'F': 66524},
    55: {'M': 69941, 'F': 73872},
    56: {'M': 69971, 'F': 73061},
    57: {'M': 67097, 'F': 69182},
    58: {'M': 72290, 'F': 74638},
    59: {'M': 72281, 'F': 78715},
    60: {'M': 73744, 'F': 82105},
    61: {'M': 69632, 'F': 76387},
    62: {'M': 64859, 'F': 71001},
    63: {'M': 64348, 'F': 71117},
    64: {'M': 58022, 'F': 65238},
    65: {'M': 60409, 'F': 69675},
    66: {'M': 52243, 'F': 57904},
    67: {'M': 45398, 'F': 51517},
    68: {'M': 49382, 'F': 55852},
    69: {'M': 34423, 'F': 39014},
    70: {'M': 38180, 'F': 43578},
    71: {'M': 37924, 'F': 44277},
    72: {'M': 37394, 'F': 43431},
    73: {'M': 37515, 'F': 44936},
    74: {'M': 30036, 'F': 36223},
    75: {'M': 26301, 'F': 31527},
    76: {'M': 26380, 'F': 32006},
    77: {'M': 25298, 'F': 31255},
    78: {'M': 29950, 'F': 36403},
    79: {'M': 23303, 'F': 29217},
    80: {'M': 18976, 'F': 25060},
    81: {'M': 17966, 'F': 24561},
    82: {'M': 14973, 'F': 21437},
    83: {'M': 12609, 'F': 19360},
    84: {'M': 10472, 'F': 17278},
    85: {'M': 9129, 'F': 16026},
    86: {'M': 6895, 'F': 13259},
    87: {'M': 5412, 'F': 10984},
    88: {'M': 4393, 'F': 9557},
    89: {'M': 3066, 'F': 7594},
    90: {'M': 2398, 'F': 6626},
    91: {'M': 2121, 'F': 5665},
    92: {'M': 1490, 'F': 4668},
    93: {'M': 1091, 'F': 3624},
    94: {'M': 673, 'F': 2454},
    95: {'M': 440, 'F': 1769},
    96: {'M': 392, 'F': 1241},
    97: {'M': 380, 'F': 1125},
    98: {'M': 191, 'F': 757},
    99: {'M': 136, 'F': 499},
    100: {'M': 200, 'F': 827},
}


class Population(object):
    def __init__(self, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        # declare class variables and insert values
        if isinstance(is_total_sum, bool):
            self.__is_total_sum = is_total_sum
        else:
            error_msg = f"is_total_sum '{is_total_sum}' should be a boolean."
            raise ValueError(error_msg)
        if isinstance(is_pos_only, bool):
            self.__is_pos_only = is_pos_only
        else:
            error_msg = f"is_pos_only '{is_pos_only}' should be a boolean."
            raise ValueError(error_msg)
        # declare class variables and insert with class methods
        self.__total = None
        self.__male = None
        self.__female = None
        self.update(total=total, male=male, female=female, sum_senior=False)
        return

    def sum_senior_pop(self):
        if not self.is_total_sum():
            raise NotImplementedError("sum_senior_pop() is not available when is_total_sum() is False")
        if isinstance(self, Stack):
            pyramid = self.get_pyramid()
            if pyramid:
                pyramid.sum_stacks()
        elif isinstance(self, AgeLayer):
            self.get_stack().sum_age_layers()
        return

    def update(self, total=None, male=None, female=None, sum_senior=True):
        # None as input indicates "keep existing values" (in case of a conflict with new inputs, nullify existing values)
        # check inputs
        total_is_num = is_number(total)
        male_is_num = is_number(male)
        female_is_num = is_number(female)
        if self.__is_pos_only:
            if total_is_num:
                if total < 0:
                    error_msg = f"total '{total}' should be equal to or greater than 0."
                    raise ValueError(error_msg)
            if male_is_num:
                if male < 0:
                    error_msg = f"male '{male}' should be equal to or greater than 0."
                    raise ValueError(error_msg)
            if female_is_num:
                if female < 0:
                    error_msg = f"female '{female}' should be equal to or greater than 0."
                    raise ValueError(error_msg)
        # insert values
        if self.__is_total_sum:
            if male_is_num and female_is_num:
                if total_is_num:
                    if total != male + female:
                        error_msg = f"male '{male}' and female '{female}' do not sum to total '{total}'."
                        raise ValueError(error_msg)
                self.update_genders(male, female, sum_senior=sum_senior)
            elif total_is_num:
                if male_is_num:
                    if total >= male:
                        self.update_genders(male, total - male, sum_senior=sum_senior)
                    else:
                        error_msg = f"male '{male}' is greater than total '{total}'."
                        raise ValueError(error_msg)
                elif female_is_num:
                    if total >= female:
                        self.update_genders(total - female, female, sum_senior=sum_senior)
                    else:
                        error_msg = f"female '{female}' is greater than total '{total}'."
                        raise ValueError(error_msg)
                else:
                    self.update_total(total, sum_senior=sum_senior)
            elif male_is_num:
                self.update_male(male, sum_senior=sum_senior)
            elif female_is_num:
                self.update_female(female, sum_senior=sum_senior)
            else:
                self.__total = None
                self.__male = None
                self.__female = None
                if sum_senior:
                    self.sum_senior_pop()
        else:
            if total is not None:
                self.update_total(total, sum_senior=sum_senior)
            if male is not None:
                self.update_male(male, sum_senior=sum_senior)
            if female is not None:
                self.update_female(female, sum_senior=sum_senior)
        return

    def update_total(self, total, sum_senior=True):
        # check and insert input: total
        if self.__is_pos_only:
            if is_pos_number(total) is False:
                error_msg = f"total '{total}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if is_number(total) is False:
                error_msg = f"total '{total}' is not a number."
                raise ValueError(error_msg)
        # insert values
        self.__total = total
        if self.__is_total_sum:
            # delete male & female unless they agree with new total
            male_is_pos_num = is_pos_number(self.__male)
            female_is_pos_num = is_pos_number(self.__female)
            if male_is_pos_num and female_is_pos_num:
                if self.__total != self.__male + self.__female:
                    # existing male and female conflict with new total
                    self.__male = None
                    self.__female = None
                # else: no action (data is complete)
            elif male_is_pos_num:
                if self.__male <= self.__total:
                    # existing male does not conflict with new total
                    self.__female = self.__total - self.__male
                else:
                    # existing male conflicts with new total
                    self.__male = None
                    self.__female = None
            elif female_is_pos_num:
                if self.__female <= self.__total:
                    # existing female does not conflict with new total
                    self.__male = self.__total - self.__female
                else:
                    # existing female conflicts with new total
                    self.__male = None
                    self.__female = None
            else:
                self.__male = None
                self.__female = None
            if sum_senior:
                self.sum_senior_pop()
        return

    def update_genders(self, male, female, sum_senior=True):
        # check and insert inputs: male, female
        if self.__is_pos_only:
            if is_pos_number(male) is False or is_pos_number(female) is False:
                error_msg = f"male '{male}' and female '{female}' are not positive numbers."
                raise ValueError(error_msg)
        else:
            if is_number(male) is False or is_number(female) is False:
                error_msg = f"male '{male}' and female '{female}' are not numbers."
                raise ValueError(error_msg)
        # insert values
        self.__male = male
        self.__female = female
        if self.__is_total_sum:
            self.__total = male + female
            if sum_senior:
                self.sum_senior_pop()
        return

    def update_male(self, male, sum_senior=True):
        # check and insert input: male
        if self.__is_pos_only:
            if not is_pos_number(male):
                error_msg = f"male '{male}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if not is_number(male):
                error_msg = f"male '{male}' is not a number."
                raise ValueError(error_msg)
        self.__male = male
        if self.__is_total_sum:
            # delete total & female unless they agree with new male
            total_is_pos_num = is_pos_number(self.__total)
            female_is_pos_num = is_pos_number(self.__female)
            if total_is_pos_num and female_is_pos_num:
                if self.__total != self.__male + self.__female:
                    # existing total and female conflict with new male
                    self.__total = None
                    self.__female = None
                # else: no action (data is complete)
            elif total_is_pos_num:
                if self.__total >= self.__male:
                    # existing total does not conflict with new male
                    self.__female = self.__total - self.__male
                else:
                    # existing total conflicts with new male
                    self.__total = None
                    self.__female = None
            elif female_is_pos_num:
                self.__total = self.__male + self.__female
            else:
                self.__total = None
                self.__female = None
            if sum_senior:
                self.sum_senior_pop()
        return

    def update_female(self, female, sum_senior=True):
        # check and insert input: female
        if self.__is_pos_only:
            if not is_pos_number(female):
                error_msg = f"female '{female}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if not is_number(female):
                error_msg = f"female '{female}' is not a number."
                raise ValueError(error_msg)
        self.__female = female
        if self.__is_total_sum:
            # delete total & male unless they agree with new female
            total_is_pos_num = is_pos_number(self.__total)
            male_is_pos_num = is_pos_number(self.__male)
            if total_is_pos_num and male_is_pos_num:
                if self.__total != self.__male + self.__female:
                    # existing total and male conflict with new female
                    self.__total = None
                    self.__male = None
                # else: no action (data is complete)
            elif total_is_pos_num:
                if self.__total >= self.__female:
                    # existing total does not conflict with new female
                    self.__male = self.__total - self.__female
                else:
                    # existing total conflicts with new female
                    self.__total = None
                    self.__male = None
            elif male_is_pos_num:
                self.__total = self.__male + self.__female
            else:
                self.__total = None
                self.__male = None
            if sum_senior:
                self.sum_senior_pop()
        return

    def is_pos_only(self):
        return self.__is_pos_only

    def is_total_sum(self):
        return self.__is_total_sum

    def get_total(self):
        return self.__total

    def get_male(self):
        return self.__male

    def get_female(self):
        return self.__female

    def is_complete(self):
        total_is_pos_num = is_pos_number(self.__total)
        male_is_pos_num = is_pos_number(self.__male)
        female_is_pos_num = is_pos_number(self.__female)
        if total_is_pos_num and male_is_pos_num and female_is_pos_num:
            complete = (self.__total == self.__male + self.__female)
        else:
            complete = False
        return complete


class Pyramid(Population):
    def __init__(self, stack_height=5, num_stacks=17, max_age_layer=100, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None, age_data=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        # declare class variables and insert values
        if isinstance(stack_height, int):
            self.__age_range = stack_height
        else:
            error_msg = f"stack_height '{stack_height}' should be an integer."
            raise ValueError(error_msg)
        if isinstance(num_stacks, int):
            self.__num_stacks = num_stacks
        else:
            error_msg = f"num_stacks '{num_stacks}' should be an integer."
            raise ValueError(error_msg)
        if isinstance(max_age_layer, int):
            self.__max_age_layer = max_age_layer
        else:
            error_msg = f"max_age_layer '{max_age_layer}' should be an integer."
            raise ValueError(error_msg)
        # declare class variables and insert values with class methods
        self.stacks = None
        self.__age_in_pyramid_idx = None
        self.build_stacks(age_data=age_data)
        return

    def __repr__(self):
        min_len = 7
        return f'[ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'

    def __getitem__(self, age: int):
        (stack_idx, age_idx) = self.__age_in_pyramid_idx[age]
        return self.stacks[stack_idx].age_layers[age_idx]

    def build_stacks(self, age_range=None, num_stacks=None, max_age_layer=None, age_data=None):
        # default inputs
        if age_range is None:
            age_range = self.__age_range
        if num_stacks is None:
            num_stacks = self.__num_stacks
        if max_age_layer is None:
            max_age_layer = self.__max_age_layer

        # check and insert input: age_range
        if not isinstance(age_range, int):
            error_msg = f"age_range '{age_range}' is not an integer."
            raise ValueError(error_msg)
        elif age_range < 1:
            error_msg = f"age_range '{age_range}' should be equal to or greater than 1."
            raise ValueError(error_msg)
        self.__age_range = age_range
        # check and insert input: num_stacks
        if not isinstance(num_stacks, int):
            error_msg = f"num_stacks '{num_stacks}' is not an integer."
            raise ValueError(error_msg)
        elif num_stacks < 1:
            error_msg = f"num_stacks '{num_stacks}' should be equal to or greater than 1."
            raise ValueError(error_msg)
        self.__num_stacks = num_stacks
        # check and insert input: max_age_layer
        if not isinstance(max_age_layer, int):
            error_msg = f"max_age_layer '{max_age_layer}' is not an integer."
            raise ValueError(error_msg)
        else:
            max_age_layer_floor = age_range * (num_stacks - 1)
            if not max_age_layer >= max_age_layer_floor:
                error_msg = f"max_age_layer '{max_age_layer}' should be equal to or greater than {max_age_layer_floor}."
                raise ValueError(error_msg)
        self.__max_age_layer = max_age_layer

        # build stacks one by one
        stacks = []
        age_to_stack = {}
        for i in range(num_stacks):
            min_age = i * age_range
            if i < num_stacks - 1:
                max_age = (i + 1) * age_range - 1
                stack = Stack(pyramid=self, min_age=min_age, max_age=max_age, is_last_stack=False, is_total_sum=super().is_total_sum(), is_pos_only=self.is_pos_only(), age_data=age_data)
                age_dict = {age: [i, j] for j, age in enumerate(range(min_age, max_age + 1))}
            else:
                stack = Stack(pyramid=self, min_age=min_age, max_age=max_age_layer, is_last_stack=True, is_total_sum=super().is_total_sum(), is_pos_only=self.is_pos_only(), age_data=age_data)
                age_dict = {age: [i, j] for j, age in enumerate(range(min_age, max_age_layer + 1))}
            stacks.append(stack)
            age_to_stack.update(age_dict)
        self.stacks = tuple(stacks)
        self.__age_in_pyramid_idx = age_to_stack
        if super().is_total_sum():
            self.sum_stacks()
        return

    def sum_stacks(self):
        if not super().is_total_sum():
            error_msg = "sum_stacks() is not available when is_total_sum() is False"
            raise NotImplementedError(error_msg)
        total = 0
        male = 0
        female = 0
        sum_total = True
        sum_gender = True
        for stack in self.stacks:
            t = stack.get_total()
            m = stack.get_male()
            f = stack.get_female()
            if sum_total:
                if is_number(t):
                    total += t
                else:
                    sum_total = False
                    total = None
            if sum_gender:
                if is_number(m) and is_number(f):
                    male += m
                    female += f
                else:
                    sum_gender = False
                    male = None
                    female = None
            if sum_total is False and sum_gender is False:
                break
        if sum_gender:
            super().update_genders(male, female, sum_senior=False)
        elif sum_total:
            super().update_total(total, sum_senior=False)
        return

    def is_age_valid(self, age: int,):
        if not isinstance(age, int):
            return False
        elif not (0 <= age <= self.__max_age_layer):
            return False
        return True


class Stack(Population):
    def __init__(self, pyramid=None, min_age=0, max_age=100, is_last_stack=True, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None, age_data=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        # declare class variables and insert values
        if isinstance(pyramid, Pyramid) or pyramid is None:
            self.__pyramid = pyramid
        else:
            error_msg = f"pyramid '{pyramid}' should be a Pyramid or None."
            raise ValueError(error_msg)
        if isinstance(min_age, int):
            self.__min_age = min_age
        else:
            error_msg = f"min_age '{min_age}' should be an integer."
            raise ValueError(error_msg)
        if isinstance(max_age, int):
            self.__max_age = max_age
        else:
            error_msg = f"max_age '{max_age}' should be an integer."
            raise ValueError(error_msg)
        if isinstance(is_last_stack, bool):
            self.__is_last_stack = is_last_stack
        else:
            error_msg = f"is_last_stack '{is_last_stack}' should be a boolean."
            raise ValueError(error_msg)
        # declare class variables and insert values with class methods
        self.age_layers = None
        self.build_age_layers(age_data=age_data)
        return

    def __repr__(self):
        min_len = 7
        age_str = f'{self.__min_age:>2}'
        if self.__is_last_stack:
            age_str += '+  '
        else:
            age_str += f'-{self.__max_age:>2}'
        return f'{age_str}:  [ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'

    def __getitem__(self, age_idx: int):
        return self.age_layers[age_idx]

    def get_pyramid(self):
        return self.__pyramid

    def build_age_layers(self, min_age=None, max_age=None, is_last_stack=None, age_data=None):
        # default inputs
        if min_age is None:
            min_age = self.__min_age
        if max_age is None:
            max_age = self.__max_age
        if is_last_stack is None:
            is_last_stack = self.__is_last_stack

        # check and insert input: min_age
        if not isinstance(min_age, int):
            error_msg = f"min_age '{min_age}' is not an integer."
            raise ValueError(error_msg)
        elif min_age < 0:
            error_msg = f"min_age '{min_age}' should be equal to or greater than 0."
            raise ValueError(error_msg)
        self.__min_age = min_age
        # check and insert input: max_age
        if not isinstance(max_age, int):
            error_msg = f"max_age '{max_age}' is not an integer."
            raise ValueError(error_msg)
        elif max_age < min_age:
            error_msg = f"max_age '{max_age}' should be equal to or greater than min_age '{min_age}'."
            raise ValueError(error_msg)
        self.__max_age = max_age
        # check and insert input: is_last_stack
        if not isinstance(is_last_stack, bool):
            error_msg = f"is_last_stack '{is_last_stack}' is not a boolean."
            raise ValueError(error_msg)
        self.__is_last_stack = is_last_stack

        # build layers one by one
        ages = []
        for age in range(min_age, max_age + 1):
            if age == max_age:
                is_last_layer = is_last_stack
            else:
                is_last_layer = False

            if age_data:
                if age in age_data.keys():
                    (total, male, female) = get_pop_data_from_dict(age_data[age])
                else:
                    total = None
                    male = None
                    female = None
            else:
                total = None
                male = None
                female = None

            age_layer = AgeLayer(self, age, is_last_layer=is_last_layer, is_total_sum=super().is_total_sum(), is_pos_only=self.is_pos_only(), total=total, male=male, female=female)
            ages.append(age_layer)
        self.age_layers = tuple(ages)
        if super().is_total_sum():
            self.sum_age_layers(sum_stacks=False)
        return

    def sum_age_layers(self, sum_stacks=True):
        if not super().is_total_sum():
            error_msg = "sum_ages() is not available when is_total_sum() is False"
            raise NotImplementedError(error_msg)
        old_total = super().get_total()
        old_male = super().get_male()
        old_female = super().get_female()

        total = 0
        male = 0
        female = 0
        sum_total = True
        sum_gender = True
        for age in self.age_layers:
            t = age.get_total()
            m = age.get_male()
            f = age.get_female()
            if sum_total:
                if is_number(t):
                    total += t
                else:
                    sum_total = False
                    total = None
            if sum_gender:
                if is_number(m) and is_number(f):
                    male += m
                    female += f
                else:
                    sum_gender = False
                    male = None
                    female = None
            if sum_total is False and sum_gender is False:
                break
        if sum_gender:
            super().update_genders(male, female, sum_senior=False)
        elif sum_total:
            super().update_total(total, sum_senior=False)

        pyramid = self.get_pyramid()
        if pyramid and super().is_total_sum() and sum_stacks:
            new_total = super().get_total()
            new_male = super().get_male()
            new_female = super().get_female()
            if (old_total != new_total) or (old_male != new_male) or (old_female != new_female):
                pyramid.sum_stacks()
        return


class AgeLayer(Population):
    def __init__(self, stack: Stack, age: int, is_last_layer=True, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        # declare class variables and insert values
        if isinstance(stack, Stack):
            self.__stack = stack
        else:
            error_msg = f"stack '{stack}' should be a Stack."
            raise ValueError(error_msg)
        if isinstance(age, int):
            self.__age = age
        else:
            error_msg = f"age '{age}' should be an integer."
            raise ValueError(error_msg)
        if isinstance(is_last_layer, bool):
            self.__last_age = is_last_layer
        else:
            error_msg = f"is_last_layer '{is_last_layer}' should be a boolean."
            raise ValueError(error_msg)
        return

    def __repr__(self):
        min_len = 7
        age_str = f'{self.__age:>3}'
        if self.__last_age:
            age_str += '+ '
        else:
            age_str += '  '
        return f'{age_str}: [ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'

    def get_stack(self):
        return self.__stack


def get_pop_data_from_dict(pop_data: dict):
    total = None
    male = None
    female = None
    if 'T' in pop_data.keys():
        total = pop_data['T']
    if 'M' in pop_data.keys():
        male = pop_data['M']
    if 'F' in pop_data.keys():
        female = pop_data['F']
    return total, male, female


def int_to_str(input_int: int, min_len=0):
    if isinstance(input_int, int):
        output_str = '{:,}'.format(input_int)
    else:
        output_str = 'N/A'
    output_str = output_str.rjust(min_len, ' ')
    return output_str


def str_to_int(input_str: str):
    if isinstance(input_str, str):
        output_str = input_str.replace(',', '')
        output_int = int(output_str)
    elif isinstance(input_str, int):
        output_int = input_str
    elif input_str is None or isna(input_str):
        output_int = None
    else:
        error_msg = f"input_str '{input_str}' is not a string."
        raise ValueError(error_msg)

    return output_int


def is_number(input_num):
    output_bool = isinstance(input_num, int) or isinstance(input_num, float)
    return output_bool


def is_pos_number(input_num):
    if is_number(input_num):
        output_bool = (input_num >= 0)
    else:
        output_bool = False
    return output_bool
