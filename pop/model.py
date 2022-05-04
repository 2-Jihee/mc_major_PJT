import pandas as pd
import numpy as np


div_name_to_div_num = {'서울': 1100000000,
                       '서울시': 1100000000,
                       '서울특별시': 1100000000,
                       }

div_num_to_div_name = {1100000000: '서울특별시',
                       }


class Population(object):
    def __init__(self, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        # declare class variables
        self.__is_total_sum = is_total_sum
        self.__is_pos_only = is_pos_only
        self.__total = None
        self.__male = None
        self.__female = None
        # insert values into class variables
        self.update(total=total, male=male, female=female)
        return

    def update(self, total=None, male=None, female=None):
        # check inputs
        total_is_num = is_number(total)
        male_is_num = is_number(male)
        female_is_num = is_number(female)
        if self.__is_pos_only:
            if total_is_num:
                if total < 0:
                    raise ValueError(f"total '{total}' should be equal to or greater than 0.")
            if male_is_num:
                if male < 0:
                    raise ValueError(f"male '{male}' should be equal to or greater than 0.")
            if female_is_num:
                if female < 0:
                    raise ValueError(f"female '{female}' should be equal to or greater than 0.")
        # insert values
        if self.__is_total_sum:
            if male_is_num and female_is_num:
                if total_is_num:
                    if total != male + female:
                        raise ValueError(f"male '{male}' and female '{female}' do not sum to total '{total}'.")
                self.update_genders(male, female)
            elif total_is_num:
                if male_is_num:
                    if total >= male:
                        self.update_genders(male, total - male)
                    else:
                        raise ValueError(f"male '{male}' is greater than total '{total}'.")
                elif female_is_num:
                    if total >= female:
                        self.update_genders(total - female, female)
                    else:
                        raise ValueError(f"female '{female}' is greater than total '{total}'.")
                else:
                    self.update_total(total)
            elif male_is_num:
                self.update_male(male)
            elif female_is_num:
                self.update_female(female)
            else:
                self.__total = None
                self.__male = None
                self.__female = None
        else:
            if total is not None:
                self.update_total(total)
            if male is not None:
                self.update_male(male)
            if female is not None:
                self.update_female(female)
        return

    def update_total(self, total):
        # check and insert input: total
        if self.__is_pos_only:
            if not is_pos_number(total):
                raise ValueError(f"total '{total}' is not a positive number.")
        else:
            if not is_number(total):
                raise ValueError(f"total '{total}' is not a number.")
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
        return

    def update_genders(self, male, female):
        # check and insert inputs: male, female
        if self.__is_pos_only:
            if is_pos_number(male) is False or is_pos_number(female) is False:
                raise ValueError(f"male '{male}' and female '{female}' are not positive numbers.")
        else:
            if is_number(male) is False or is_number(female) is False:
                raise ValueError(f"male '{male}' and female '{female}' are not numbers.")
        # insert values
        self.__male = male
        self.__female = female
        if self.__is_total_sum:
            self.__total = male + female
        return

    def update_male(self, male):
        # check and insert input: male
        if self.__is_pos_only:
            if not is_pos_number(male):
                raise ValueError(f"male '{male}' is not a positive number.")
        else:
            if not is_number(male):
                raise ValueError(f"male '{male}' is not a number.")
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
        return

    def update_female(self, female):
        # check and insert input: female
        if self.__is_pos_only:
            if not is_pos_number(female):
                raise ValueError(f"female '{female}' is not a positive number.")
        else:
            if not is_number(female):
                raise ValueError(f"female '{female}' is not a number.")
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
    def __init__(self, age_range=5, num_stacks=17, max_age_layer=100, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        # declare class variables
        self.__age_range = age_range
        self.__num_stacks = num_stacks
        self.__max_age_layer = max_age_layer
        self.stacks = None
        self.__age_to_stack = None
        # insert values into class variables
        self.build_stacks()
        return

    def __repr__(self):
        min_len = 7
        return f'[ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'

    def __getitem__(self, age):
        (stack_idx, age_idx) = self.__age_to_stack[age]
        return self.stacks[stack_idx].ages[age_idx]

    def build_stacks(self, age_range=None, num_stacks=None, max_age_layer=None):
        # default inputs
        if age_range is None:
            age_range = self.__age_range
        if num_stacks is None:
            num_stacks = self.__num_stacks
        if max_age_layer is None:
            max_age_layer = self.__max_age_layer

        # check and insert input: age_range
        if not isinstance(age_range, int):
            raise ValueError(f"age_range '{age_range}' is not an integer.")
        elif age_range < 1:
            raise ValueError(f"age_range '{age_range}' should be equal to or greater than 1.")
        self.__age_range = age_range
        # check and insert input: num_stacks
        if not isinstance(num_stacks, int):
            raise ValueError(f"num_stacks '{num_stacks}' is not an integer.")
        elif num_stacks < 1:
            raise ValueError(f"num_stacks '{num_stacks}' should be equal to or greater than 1.")
        self.__num_stacks = num_stacks
        # check and insert input: max_age_layer
        if not isinstance(max_age_layer, int):
            raise ValueError(f"max_age_layer '{max_age_layer}' is not an integer.")
        else:
            max_age_layer_floor = age_range * (num_stacks - 1)
            if not max_age_layer >= max_age_layer_floor:
                raise ValueError(f"max_age_layer '{max_age_layer}' should be equal to or greater than {max_age_layer_floor}.")
        self.__max_age_layer = max_age_layer

        # build stacks one by one
        stacks = []
        age_to_stack = {}
        for i in range(num_stacks):
            if i < num_stacks - 1:
                stack = Stack(i * age_range, (i + 1) * age_range - 1, is_total_sum=self.is_total_sum(), is_pos_only=self.is_pos_only())
                age_dict = {age: [i, j] for j, age in enumerate(range(i * age_range, (i + 1) * age_range))}
            else:
                stack = Stack(i * age_range, max_age_layer, is_last_stack=True, is_total_sum=self.is_total_sum(), is_pos_only=self.is_pos_only())
                age_dict = {age: [i, j] for j, age in enumerate(range(i * age_range, max_age_layer + 1))}
            stacks.append(stack)
            age_to_stack.update(age_dict)
        self.stacks = tuple(stacks)
        self.__age_to_stack = age_to_stack
        return

    def update_age_genders(self, age: int, male, female):
        if not isinstance(age, int):
            raise ValueError(f"age '{age}' is not an integer.")
        elif not (0 <= age <= self.__max_age_layer):
            raise ValueError(f"age '{age}' is out of range (min=0, max={self.__max_age_layer}).")
        (stack_idx, age_idx) = self.__age_to_stack[age]
        self.stacks[stack_idx].ages[age_idx].update_genders(male, female)
        if self.is_total_sum():
            self.stacks[stack_idx].sum_ages()
            self.sum_stacks()

    def is_ages_complete(self):
        is_complete = True
        for stack in self.stacks:
            for age in stack.ages:
                if not age.is_complete():
                    is_complete = False
                    break
        return is_complete

    def sum_stacks(self):
        if not self.is_total_sum():
            raise NotImplementedError("sum_stacks() is not available when __is_total_sum is False")
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
            self.update_genders(male, female)
        elif sum_total:
            self.update_total(total)
        return


class Stack(Population):
    def __init__(self, min_age, max_age, is_last_stack=False, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        # declare class variables
        self.__min_age = min_age
        self.__max_age = max_age
        self.__is_last_stack = is_last_stack
        self.ages = None
        # insert values into class variables
        self.build_age_layers()
        return

    def __repr__(self):
        min_len = 7
        age_str = f'{self.__min_age:>2}'
        if self.__is_last_stack:
            age_str += '+  '
        else:
            age_str += f'-{self.__max_age:>2}'
        return f'{age_str}:  [ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'

    def build_age_layers(self, min_age=None, max_age=None, is_last_stack=None):
        # default inputs
        if min_age is None:
            min_age = self.__min_age
        if max_age is None:
            max_age = self.__max_age
        if is_last_stack is None:
            is_last_stack = self.__is_last_stack

        # check and insert input: min_age
        if not isinstance(min_age, int):
            raise ValueError(f"min_age '{min_age}' is not an integer.")
        elif min_age < 0:
            raise ValueError(f"min_age '{min_age}' should be equal to or greater than 0.")
        self.__min_age = min_age
        # check and insert input: max_age
        if not isinstance(max_age, int):
            raise ValueError(f"max_age '{max_age}' is not an integer.")
        elif max_age < min_age:
            raise ValueError(f"max_age '{max_age}' should be equal to or greater than min_age '{min_age}'.")
        self.__max_age = max_age
        # check and insert input: is_last_stack
        if not isinstance(is_last_stack, bool):
            raise ValueError(f"is_last_stack '{is_last_stack}' is not a boolean.")
        self.__is_last_stack = is_last_stack

        # build layers one by one
        ages = [AgeLayer(age, is_total_sum=self.is_total_sum(), is_pos_only=self.is_pos_only()) for age in range(min_age, max_age)]
        ages.append(AgeLayer(max_age, last_age=is_last_stack, is_total_sum=self.is_total_sum(), is_pos_only=self.is_pos_only()))
        self.ages = tuple(ages)
        return

    def sum_ages(self):
        if not self.is_total_sum():
            raise NotImplementedError("sum_ages() is not available when __is_total_sum is False")
        total = 0
        male = 0
        female = 0
        sum_total = True
        sum_gender = True
        for age in self.ages:
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
            self.update_genders(male, female)
        elif sum_total:
            self.update_total(total)

        return


class AgeLayer(Population):
    def __init__(self, age, last_age=False, is_total_sum=True, is_pos_only=True, total=None, male=None, female=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, total=total, male=male, female=female)
        self.__age = age
        self.__last_age = last_age
        return

    def __repr__(self):
        min_len = 7
        age_str = f'{self.__age:>3}'
        if self.__last_age:
            age_str += '+ '
        else:
            age_str += '  '
        return f'{age_str}: [ Total: {int_to_str(super().get_total(), min_len)},    M: {int_to_str(super().get_male(), min_len)},    F: {int_to_str(super().get_female(), min_len)} ]'


def int_to_str(input_int: int, min_len=0):
    if isinstance(input_int, int):
        output_str = '{:,}'.format(input_int)
    else:
        output_str = 'N/A'
    output_str = output_str.rjust(min_len, ' ')
    return output_str


def is_number(input_num):
    output_bool = isinstance(input_num, int) or isinstance(input_num, float)
    return output_bool


def is_pos_number(input_num):
    if is_number(input_num):
        output_bool = (input_num >= 0)
    else:
        output_bool = False
    return output_bool
