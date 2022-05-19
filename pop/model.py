from pandas import isna
from copy import deepcopy

min_print_len = 9


class Population(object):
    def __init__(self, parent=None, is_total_sum=True, is_pos_only=True, fill_zeros=False, total=None, male=None, female=None):
        # check the input and declare a class variable with value: parent
        if isinstance(parent, Population):
            self.__parent = parent
        else:
            self.__parent = None

        # check the input and declare a class variable with value: is_total_sum
        if not isinstance(is_total_sum, bool):
            error_msg = f"is_total_sum '{is_total_sum}' should be a boolean."
            raise ValueError(error_msg)
        self.__is_total_sum = is_total_sum

        # check the input and declare a class variable with value: is_pos_only
        if not isinstance(is_pos_only, bool):
            error_msg = f"is_pos_only '{is_pos_only}' should be a boolean."
            raise ValueError(error_msg)
        self.__is_pos_only = is_pos_only

        # declare class variables and insert values via class methods
        if fill_zeros:
            self.__total = 0
            self.__male = 0
            self.__female = 0
        else:
            self.__total = None
            self.__male = None
            self.__female = None
            self.insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        # declare empty variables
        self.children = None
        return

    def copy(self):
        return deepcopy(self)

    def is_pos_only(self):
        return self.__is_pos_only

    def is_total_sum(self):
        return self.__is_total_sum

    def has_parent(self):
        return isinstance(self.__parent, Population)

    def get_parent(self):
        return self.__parent

    def has_children(self):
        return isinstance(self.children, tuple)

    def get_total(self):
        return self.__total

    def get_male(self):
        return self.__male

    def get_female(self):
        return self.__female

    def delete_data(self, delete_parent_data=None, delete_children_data=None):
        if delete_parent_data is None:
            delete_parent_data = False
        if delete_children_data is None:
            delete_parent_data = self.__is_total_sum

        self.__total = None
        self.__male = None
        self.__female = None
        if delete_parent_data:
            self.delete_parent_data()
        if delete_children_data:
            self.delete_children_data()
        return

    def delete_parent_data(self):
        if self.has_parent():
            parent = self.__parent()
            parent.delete_data(delete_parent_data=True, delete_children_data=False)
        return

    def delete_children_data(self):
        if self.has_children():
            for child in self.children:
                child.delete_data(delete_parent_data=False, delete_children_data=True)
        return

    def insert(self, total=None, male=None, female=None, delete_children=True, insert_parent=True):
        # None as input indicates "keep existing values" (in case of conflict with new inputs, nullify existing values)
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

        # decide on which insert function to use
        if not self.__is_total_sum:
            if total is not None:
                self.insert_total(total, delete_children=False, insert_parent=False)
            if male is not None:
                self.insert_male(male, delete_children=False, insert_parent=False)
            if female is not None:
                self.insert_female(female, delete_children=False, insert_parent=False)
        else:
            if male_is_num and female_is_num:
                if total_is_num:
                    if total != male + female:
                        error_msg = f"male '{male}' and female '{female}' do not sum to total '{total}'."
                        raise ValueError(error_msg)
                self.insert_sexes(male, female, delete_children=delete_children, insert_parent=insert_parent)
            elif total_is_num:
                self.insert_total(total, delete_children=delete_children, insert_parent=insert_parent)
            elif male_is_num:
                self.insert_male(male, delete_children=delete_children, insert_parent=insert_parent)
            elif female_is_num:
                self.insert_female(female, delete_children=delete_children, insert_parent=insert_parent)
        return

    def insert_total(self, total, delete_children=True, insert_parent=True):
        # in case of conflict with new input, nullify existing values
        # check input
        if self.__is_pos_only:
            if is_pos_number(total) is False:
                error_msg = f"total '{total}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if is_number(total) is False:
                error_msg = f"total '{total}' is not a number."
                raise ValueError(error_msg)

        # return if there is no change
        if total == self.__total:
            return

        # insert value & delete children
        self.__total = total
        if delete_children:
            self.delete_children_data()

        # when is_total_sum is True
        if self.__is_total_sum:
            male_is_num = is_number(self.__male)
            female_is_num = is_number(self.__female)
            if male_is_num and not female_is_num:
                if (self.__is_pos_only is False) or (self.__male <= self.__total):
                    # existing male does not conflict with new total
                    self.__female = self.__total - self.__male
                else:
                    # existing male conflicts with new total
                    self.__male = None
                    self.__female = None
            elif female_is_num and not male_is_num:
                if (self.__is_pos_only is False) or (self.__female <= self.__total):
                    # existing female does not conflict with new total
                    self.__male = self.__total - self.__female
                else:
                    # existing female conflicts with new total
                    self.__male = None
                    self.__female = None
            else:
                self.__male = None
                self.__female = None

            if insert_parent:
                self.insert_parent_via_sum()
        return

    def insert_sexes(self, male, female, delete_children=True, insert_parent=True):
        # in case of conflict with new inputs, nullify existing values
        # check inputs
        if self.__is_pos_only:
            if is_pos_number(male) is False or is_pos_number(female) is False:
                error_msg = f"male '{male}' or female '{female}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if is_number(male) is False or is_number(female) is False:
                error_msg = f"male '{male}' or female '{female}' is not a number."
                raise ValueError(error_msg)

        # return if there is no change
        if male == self.__male and female == self.__female:
            return

        # insert values & delete children
        self.__male = male
        self.__female = female
        if delete_children:
            self.delete_children_data()

        # when is_total_sum is True
        if self.__is_total_sum:
            self.__total = male + female

            if insert_parent:
                self.insert_parent_via_sum()
        return

    def insert_male(self, male, delete_children=True, insert_parent=True):
        # in case of conflict with new input, nullify existing values
        # check inputs
        if self.__is_pos_only:
            if not is_pos_number(male):
                error_msg = f"male '{male}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if not is_number(male):
                error_msg = f"male '{male}' is not a number."
                raise ValueError(error_msg)

        # return if there is no change
        if male == self.__male:
            return

        # insert values & delete children
        self.__male = male
        if delete_children:
            self.delete_children_data()

        # when is_total_sum is True
        if self.__is_total_sum:
            total_is_num = is_number(self.__total)
            female_is_num = is_number(self.__female)
            if total_is_num and not female_is_num:
                if (self.__is_pos_only is False) or (self.__total >= self.__male):
                    # existing total does not conflict with new male
                    self.__female = self.__total - self.__male
                else:
                    # existing total conflicts with new male
                    self.__total = None
                    self.__female = None
            elif female_is_num and not total_is_num:
                self.__total = self.__male + self.__female
            else:
                self.__total = None
                self.__female = None

            if insert_parent:
                self.insert_parent_via_sum()
        return

    def insert_female(self, female, delete_children=True, insert_parent=True):
        # in case of conflict with new input, nullify existing values
        # check inputs
        if self.__is_pos_only:
            if not is_pos_number(female):
                error_msg = f"female '{female}' is not a positive number."
                raise ValueError(error_msg)
        else:
            if not is_number(female):
                error_msg = f"female '{female}' is not a number."
                raise ValueError(error_msg)

        # return if there is no change
        if female == self.__female:
            return

        # insert values & delete children
        self.__female = female
        if delete_children:
            self.delete_children_data()

        # when is_total_sum is True
        if self.__is_total_sum:
            total_is_num = is_number(self.__total)
            male_is_num = is_number(self.__male)
            if total_is_num and not male_is_num:
                if (self.__is_pos_only is False) or (self.__total >= self.__female):
                    # existing total does not conflict with new female
                    self.__male = self.__total - self.__female
                else:
                    # existing total conflicts with new female
                    self.__total = None
                    self.__male = None
            elif male_is_num and not total_is_num:
                self.__total = self.__male + self.__female
            else:
                self.__total = None
                self.__male = None

            if insert_parent:
                self.insert_parent_via_sum()
        return

    def sum_children(self):
        if not self.__is_total_sum:
            error_msg = "sum_children() is not available when is_total_sum is False."
            raise NotImplementedError(error_msg)
        if not self.has_children():
            error_msg = f"No children can be found."
            raise NotImplementedError(error_msg)

        total = 0
        male = 0
        female = 0
        for child in self.children:
            t = child.get_total()
            m = child.get_male()
            f = child.get_female()
            if total is not None:
                if is_number(t):
                    total += t
                else:
                    total = None
            if male is not None:
                if is_number(m):
                    male += m
                else:
                    male = None
            if female is not None:
                if is_number(f):
                    female += f
                else:
                    female = None
            if total is None and male is None and female is None:
                break
        return total, male, female

    def insert_via_sum_children(self, insert_parent=True):
        if not self.__is_total_sum:
            error_msg = "update_via_sum() is not available when is_total_sum is False"
            raise NotImplementedError(error_msg)
        if not self.has_children():
            return

        (total, male, female) = self.sum_children()
        if not (male is None or female is None):
            # total is automatically updated
            self.insert_sexes(male, female, delete_children=False, insert_parent=insert_parent)
        elif total is not None:
            self.insert_total(total, delete_children=False, insert_parent=insert_parent)
        elif male is not None:
            self.insert_male(male, delete_children=False, insert_parent=insert_parent)
        elif female is not None:
            self.insert_female(female, delete_children=False, insert_parent=insert_parent)
        return

    def insert_parent_via_sum(self):
        if not self.__is_total_sum:
            error_msg = "sum_senior_pop() is not available when is_total_sum is False."
            raise NotImplementedError(error_msg)
        if not self.has_parent():
            return

        self.__parent.insert_via_sum_children()
        return


class Pyramid(Population):
    def __init__(self, first_stack_height=5, mid_stack_height=5, num_stacks=17, last_age=100,
                 is_total_sum=True, is_pos_only=True, fill_zeros=False, total=None, male=None, female=None, age_data=None, stack_data=None):
        super().__init__(is_total_sum=is_total_sum, is_pos_only=is_pos_only, fill_zeros=fill_zeros, total=total, male=male, female=female)
        self.__first_stack_height = None
        self.__mid_stack_height = None
        self.__num_stacks = None
        self.__last_age = None
        self.__insert_stack_variables(first_stack_height=first_stack_height, mid_stack_height=mid_stack_height, num_stacks=num_stacks, last_age=last_age)
        # declare class variables and insert values with class methods
        self.__stacks_idx = None
        self.__age_layers_idx = None
        self.generate_stacks(fill_zeros=fill_zeros, age_data=age_data, stack_data=stack_data)
        return

    def __repr__(self):
        total = super().get_total()
        male = super().get_male()
        female = super().get_female()

        if all(isinstance(value, int) for value in [total, male, female]):
            total_str = int_to_str(total, min_print_len)
            male_str = int_to_str(male, min_print_len)
            female_str = int_to_str(female, min_print_len)
        elif is_number(total) and is_number(male) and is_number(female):
            if all(abs(value) <= 1 for value in [total, male, female]):
                total_str = f'{total:8.4%}'
                male_str = f'{male:8.4%}'
                female_str = f'{female:8.4%}'
            else:
                total_str = f'{total:12,.2f}'
                male_str = f'{male:12,.2f}'
                female_str = f'{female:12,.2f}'
        else:
            total_str = str(total)
            male_str = str(male)
            female_str = str(female)

        return f'[ Total: {total_str},    M: {male_str},    F: {female_str} ]'

    def __getitem__(self, idx):
        if isinstance(idx, int):
            (stack_idx, age_idx) = self.__age_layers_idx[idx]
            output = self.children[stack_idx].children[age_idx]
        elif idx in self.__stacks_idx.keys():
            stack_idx = self.__stacks_idx[idx]
            output = self.children[stack_idx]
        else:
            error_msg = f"Unavailable attribute index '{idx}'."
            raise AttributeError(error_msg)
        return output

    def __insert_stack_variables(self, first_stack_height=None, mid_stack_height=None, num_stacks=None, last_age=None):
        # check new input or load existing input: first_stack_height
        if first_stack_height is not None:
            if not isinstance(first_stack_height, int):
                error_msg = f"first_stack_height '{first_stack_height}' should be an integer."
                raise ValueError(error_msg)
            elif first_stack_height < 1:
                error_msg = f"first_stack_height '{first_stack_height}' should be equal to or greater than 1."
                raise ValueError(error_msg)
        else:
            first_stack_height = self.__first_stack_height
        # check new input or load existing input: mid_stack_height
        if mid_stack_height is not None:
            if not isinstance(mid_stack_height, int):
                error_msg = f"mid_stack_height '{mid_stack_height}' should be an integer."
                raise ValueError(error_msg)
            elif mid_stack_height < 1:
                error_msg = f"mid_stack_height '{mid_stack_height}' should be equal to or greater than 1."
                raise ValueError(error_msg)
        else:
            mid_stack_height = self.__mid_stack_height
        # check new input or load existing input: num_stacks
        if num_stacks is not None:
            if not isinstance(num_stacks, int):
                error_msg = f"num_stacks '{num_stacks}' should be an integer."
                raise ValueError(error_msg)
            elif num_stacks < 2:
                error_msg = f"num_stacks '{num_stacks}' should be equal to or greater than 2."
                raise ValueError(error_msg)
        else:
            num_stacks = self.__num_stacks
        # check new input or load existing input: last_age
        if last_age is not None:
            if not isinstance(last_age, int):
                error_msg = f"last_age '{last_age}' is not an integer."
                raise ValueError(error_msg)
        else:
            last_age = self.__last_age
        # check inputs on a collective basis
        last_stack_start_age = first_stack_height + mid_stack_height * (num_stacks - 2)
        if last_stack_start_age >= 100:
            error_msg = f"last_stack_start_age '{last_stack_start_age}' should be less than 100."
            raise ValueError(error_msg)
        if last_age < last_stack_start_age:
            error_msg = f"last_age '{last_age}' should be equal to or greater than last_stack_start_age '{last_stack_start_age}'."
            raise ValueError(error_msg)
        # insert values
        self.__first_stack_height = first_stack_height
        self.__mid_stack_height = mid_stack_height
        self.__num_stacks = num_stacks
        self.__last_age = last_age

    def generate_stacks(self, first_stack_height=None, mid_stack_height=None, num_stacks=None, last_age=None, fill_zeros=False, age_data=None, stack_data=None):
        if not all(value is None for value in [first_stack_height, mid_stack_height, num_stacks, last_age]):
            self.__insert_stack_variables(first_stack_height=first_stack_height, mid_stack_height=mid_stack_height, num_stacks=num_stacks, last_age=last_age)
        # build stacks one by one
        stacks = []
        stack_to_idx = {}
        age_to_idx = {}
        stack_start_age = 0
        for i in range(self.__num_stacks):
            if i == (self.__num_stacks - 1):
                # last stack
                stack_end_age = self.__last_age
                is_last_stack = True
                stack_key = f"{stack_start_age:0>2}+"
            else:
                if i == 0:
                    # first stack
                    stack_end_age = stack_start_age + self.__first_stack_height - 1
                else:
                    # mid stack
                    stack_end_age = stack_start_age + self.__mid_stack_height - 1
                is_last_stack = False
                stack_key = f"{stack_start_age:0>2}{stack_end_age:0>2}"

            if fill_zeros is False and stack_data is not None:
                if stack_key in stack_data.keys():
                    (total, male, female) = get_pop_data_from_dict(stack_data[stack_key])
                else:
                    total = None
                    male = None
                    female = None
            else:
                total = None
                male = None
                female = None

            stack = Stack(parent=self, min_age=stack_start_age, max_age=stack_end_age, is_last_stack=is_last_stack, is_total_sum=super().is_total_sum(), is_pos_only=self.is_pos_only(),
                          fill_zeros=fill_zeros, total=total, male=male, female=female, age_data=age_data)
            stacks.append(stack)
            age_dict = {age: [i, j] for j, age in enumerate(range(stack_start_age, stack_end_age + 1))}
            stack_to_idx.update({stack_key: i})
            age_to_idx.update(age_dict)
            # pass variable to next loop
            stack_start_age = stack_end_age + 1
        self.children = tuple(stacks)
        self.__stacks_idx = stack_to_idx
        self.__age_layers_idx = age_to_idx

        if super().is_total_sum():
            super().insert_via_sum_children(insert_parent=False)
        return

    def get_first_stack_height(self):
        return self.__first_stack_height

    def get_mid_stack_height(self):
        return self.__mid_stack_height

    def get_num_stacks(self):
        return self.__num_stacks

    def get_last_age(self):
        return self.__last_age

    def get_stack_in_pyramid_idx(self):
        return self.__stacks_idx

    def add_one_year_and_birth(self, age_0_total=None, age_0_male=None, age_0_female=None):
        if not super().is_total_sum():
            error_msg = "add_one_year() is not available when is_total_sum is False."
            raise NotImplementedError(error_msg)

        for age in range(self.__last_age, 0, -1):
            if age == self.__last_age:
                if is_number(self[age].get_male()) and is_number(self[age-1].get_male()):
                    male = self[age].get_male() + self[age-1].get_male()
                else:
                    male = None
                if is_number(self[age].get_female()) and is_number(self[age-1].get_female()):
                    female = self[age].get_female() + self[age-1].get_female()
                else:
                    female = None
                if is_number(male) and is_number(female):
                    total = None
                elif is_number(self[age].get_total()) and is_number(self[age-1].get_total()):
                    total = self[age].get_total() + self[age-1].get_total()
                else:
                    total = None
            else:
                total = self[age-1].get_total()
                male = self[age-1].get_male()
                female = self[age-1].get_female()

            self[age].insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        self[0].insert(total=age_0_total, male=age_0_male, female=age_0_female, delete_children=False, insert_parent=False)
        for child in self.children:
            child.insert_via_sum_children(insert_parent=False)
        self.insert_via_sum_children()

    def add_pyramid(self, pyramid):
        if not super().is_total_sum():
            error_msg = "add_pyramid() is not available when is_total_sum is False."
            raise NotImplementedError(error_msg)

        if self.__last_age == pyramid.get_last_age():
            for age in range(self.__last_age + 1):
                if is_number(self[age].get_male()) and is_number(pyramid[age].get_male()):
                    male = self[age].get_male() + pyramid[age].get_male()
                else:
                    male = None
                if is_number(self[age].get_female()) and is_number(pyramid[age].get_female()):
                    female = self[age].get_female() + pyramid[age].get_female()
                else:
                    female = None
                if not (male is None or female is None):
                    total = None
                elif is_number(self[age].get_total()) and is_number(pyramid[age].get_total()):
                    total = self[age].get_total() + pyramid[age].get_total()
                else:
                    total = None

                self[age].insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        if (self.__first_stack_height == pyramid.get_first_stack_height()) and (self.__mid_stack_height == pyramid.get_mid_stack_height()) and (self.__num_stacks == pyramid.get_num_stacks()):
            for value in self.__stacks_idx.values():
                if is_number(self.children[value].get_male()) and is_number(pyramid.children[value].get_male()):
                    male = self.children[value].get_male() + pyramid.children[value].get_male()
                else:
                    male = None
                if is_number(self.children[value].get_female()) and is_number(pyramid.children[value].get_female()):
                    female = self.children[value].get_female() + pyramid.children[value].get_female()
                else:
                    female = None
                if is_number(male) and is_number(female):
                    total = None
                elif is_number(self.children[value].get_total()) and is_number(pyramid.children[value].get_total()):
                    total = self.children[value].get_total() + pyramid.children[value].get_total()
                else:
                    total = None

                self.children[value].insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        if is_number(self.get_male()) and is_number(pyramid.get_male()):
            male = self.get_male() + pyramid.get_male()
        else:
            male = None
        if is_number(self.get_female()) and is_number(pyramid.get_female()):
            female = self.get_female() + pyramid.get_female()
        else:
            female = None
        if is_number(male) and is_number(female):
            total = None
        elif is_number(self.get_total()) and is_number(pyramid.get_total()):
            total = self.get_total() + pyramid.get_total()
        else:
            total = None

        self.insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)
        for child in self.children:
            child.insert_via_sum_children(insert_parent=False)
        self.insert_via_sum_children()
        return

    def subtract_pyramid(self, pyramid):
        if not super().is_total_sum():
            error_msg = "add_pyramid() is not available when is_total_sum is False."
            raise NotImplementedError(error_msg)

        if self.__last_age == pyramid.get_last_age():
            for age in range(self.__last_age + 1):
                if is_number(self[age].get_male()) and is_number(pyramid[age].get_male()):
                    male = self[age].get_male() - pyramid[age].get_male()
                else:
                    male = None
                if is_number(self[age].get_female()) and is_number(pyramid[age].get_female()):
                    female = self[age].get_female() - pyramid[age].get_female()
                else:
                    female = None
                if not (male is None or female is None):
                    total = None
                elif is_number(self[age].get_total()) and is_number(pyramid[age].get_total()):
                    total = self[age].get_total() - pyramid[age].get_total()
                else:
                    total = None

                self[age].insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        if (self.__first_stack_height == pyramid.get_first_stack_height()) and (self.__mid_stack_height == pyramid.get_mid_stack_height()) and (self.__num_stacks == pyramid.get_num_stacks()):
            for value in self.__stacks_idx.values():
                if is_number(self.children[value].get_male()) and is_number(pyramid.children[value].get_male()):
                    male = self.children[value].get_male() - pyramid.children[value].get_male()
                else:
                    male = None
                if is_number(self.children[value].get_female()) and is_number(pyramid.children[value].get_female()):
                    female = self.children[value].get_female() - pyramid.children[value].get_female()
                else:
                    female = None
                if is_number(male) and is_number(female):
                    total = None
                elif is_number(self.children[value].get_total()) and is_number(pyramid.children[value].get_total()):
                    total = self.children[value].get_total() - pyramid.children[value].get_total()
                else:
                    total = None

                self.children[value].insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)

        if is_number(self.get_male()) and is_number(pyramid.get_male()):
            male = self.get_male() - pyramid.get_male()
        else:
            male = None
        if is_number(self.get_female()) and is_number(pyramid.get_female()):
            female = self.get_female() - pyramid.get_female()
        else:
            female = None
        if is_number(male) and is_number(female):
            total = None
        elif is_number(self.get_total()) and is_number(pyramid.get_total()):
            total = self.get_total() - pyramid.get_total()
        else:
            total = None

        self.insert(total=total, male=male, female=female, delete_children=False, insert_parent=False)
        for child in self.children:
            child.insert_via_sum_children(insert_parent=False)
        self.insert_via_sum_children()
        return

    def fill_age_layers(self):
        for stack in self.children:
            total = stack.get_total()
            male = stack.get_male()
            female = stack.get_female()
            if (total is None or male is None or female is None):
                continue

            male_ratio = male / total
            female_ratio = female / total
            for age_layer in stack.children:
                t = age_layer.get_total()
                if t is not None:
                    age_layer.insert_sexes(t * male_ratio, t * female_ratio, insert_parent=False)

            stack.insert_via_sum_children(insert_parent=False)

        self.insert_via_sum_children()
        return

    def calc_rate(self, delta_pyramid, age_lim=89):
        if is_number(delta_pyramid.get_total()) and is_number(self.get_total()) and self.get_total() != 0:
            total = delta_pyramid.get_total() / self.get_total()
        else:
            total = None
        if is_number(delta_pyramid.get_male()) and is_number(self.get_male()) and self.get_male() != 0:
            male = delta_pyramid.get_male() / self.get_male()
        else:
            male = None
        if is_number(delta_pyramid.get_female()) and is_number(self.get_female()) and self.get_female() != 0:
            female = delta_pyramid.get_female() / self.get_female()
        else:
            female = None

        if (self.__first_stack_height == delta_pyramid.get_first_stack_height()) \
                and (self.__mid_stack_height == delta_pyramid.get_mid_stack_height()) \
                and (self.__num_stacks == delta_pyramid.get_num_stacks()):
            stack_data = {}
            for key, value in self.__stacks_idx.items():
                if is_number(delta_pyramid.children[value].get_total()) and is_number(self.children[value].get_total()) and self.children[value].get_total() != 0:
                    t = delta_pyramid.children[value].get_total() / self.children[value].get_total()
                else:
                    t = None
                if is_number(delta_pyramid.children[value].get_male()) and is_number(self.children[value].get_male()) and self.children[value].get_male() != 0:
                    m = delta_pyramid.children[value].get_male() / self.children[value].get_male()
                else:
                    m = None
                if is_number(delta_pyramid.children[value].get_female()) and is_number(self.children[value].get_female()) and self.children[value].get_female() != 0:
                    f = delta_pyramid.children[value].get_female() / self.children[value].get_female()
                else:
                    f = None
                stack_data[key] = {'total': t, 'male': m, 'female': f}
        else:
            stack_data = None

        if self.__last_age == delta_pyramid.get_last_age():
            age_data = {}
            for age in range(min(age_lim, self.__last_age) + 1):
                if is_number(delta_pyramid[age].get_total()) and is_number(self[age].get_total()) and self[age].get_total() != 0:
                    t = delta_pyramid[age].get_total() / self[age].get_total()
                else:
                    t = None
                if is_number(delta_pyramid[age].get_male()) and is_number(self[age].get_male()) and self[age].get_male() != 0:
                    m = delta_pyramid[age].get_male() / self[age].get_male()
                else:
                    m = None
                if is_number(delta_pyramid[age].get_female()) and is_number(self[age].get_female()) and self[age].get_female() != 0:
                    f = delta_pyramid[age].get_female() / self[age].get_female()
                else:
                    f = None
                age_data[age] = {'total': t, 'male': m, 'female': f}

            max_age = self.__last_age
            while age_lim < max_age:
                min_age = max(age_lim + 1, max_age - 3)
                t_delta = 0
                t_self_sum = 0
                t_age = []
                m_delta = 0
                m_self_sum = 0
                m_age = []
                f_delta = 0
                f_self_sum = 0
                f_age = []
                for age in range(min_age, max_age + 1):
                    if is_number(delta_pyramid[age].get_total()) and is_number(self[age].get_total()):
                        t_delta += delta_pyramid[age].get_total()
                        t_self_sum += self[age].get_total()
                        t_age.append(age)
                    if is_number(delta_pyramid[age].get_male()) and is_number(self[age].get_male()):
                        m_delta += delta_pyramid[age].get_male()
                        m_self_sum += self[age].get_male()
                        m_age.append(age)
                    if is_number(delta_pyramid[age].get_female()) and is_number(self[age].get_female()):
                        f_delta += delta_pyramid[age].get_female()
                        f_self_sum += self[age].get_female()
                        f_age.append(age)

                if t_self_sum != 0:
                    t_rate = t_delta / t_self_sum
                else:
                    t_rate = None
                if m_self_sum != 0:
                    m_rate = m_delta / m_self_sum
                else:
                    m_rate = None
                if f_self_sum != 0:
                    f_rate = f_delta / f_self_sum
                else:
                    f_rate = None

                for age in range(min_age, max_age + 1):
                    age_data[age] = {}
                    if age in t_age:
                        t = t_rate
                    else:
                        t = None
                    if age in m_age:
                        m = m_rate
                    else:
                        m = None
                    if age in f_age:
                        f = f_rate
                    else:
                        f = None
                    age_data[age] = {'total': t, 'male': m, 'female': f}
                # pass to next loop
                max_age = min_age - 1
        else:
            age_data = None

        rate_pyramid = Pyramid(first_stack_height=self.__first_stack_height, mid_stack_height=self.__mid_stack_height, num_stacks=self.__num_stacks, last_age=self.__last_age,
                               is_total_sum=False, is_pos_only=delta_pyramid.is_pos_only(), total=total, male=male, female=female, age_data=age_data, stack_data=stack_data)
        return rate_pyramid

    def multiply_rate(self, rate_pyramid):
        if is_number(self.get_male()) and is_number(rate_pyramid.get_male()):
            male = self.get_male() * rate_pyramid.get_male()
        else:
            male = None
        if is_number(self.get_female()) and is_number(rate_pyramid.get_female()):
            female = self.get_female() * rate_pyramid.get_female()
        else:
            female = None
        if is_number(male) and is_number(female):
            total = None
        elif is_number(self.get_total()) and is_number(rate_pyramid.get_total()):
            total = self.get_total() * rate_pyramid.get_total()
        else:
            total = None

        if (self.__first_stack_height == rate_pyramid.get_first_stack_height()) \
                and (self.__mid_stack_height == rate_pyramid.get_mid_stack_height()) \
                and (self.__num_stacks == rate_pyramid.get_num_stacks()):
            stack_data = {}
            for key, value in self.__stacks_idx.items():
                if is_number(self.children[value].get_male()) and is_number(rate_pyramid.children[value].get_male()):
                    m = self.children[value].get_male() * rate_pyramid.children[value].get_male()
                else:
                    m = None
                if is_number(self.children[value].get_female()) and is_number(rate_pyramid.children[value].get_female()):
                    f = self.children[value].get_female() * rate_pyramid.children[value].get_female()
                else:
                    f = None
                if is_number(m) and is_number(f):
                    t = None
                elif is_number(self.children[value].get_total()) and is_number(rate_pyramid.children[value].get_total()):
                    t = self.children[value].get_total() * rate_pyramid.children[value].get_total()
                else:
                    t = None
                stack_data[key] = {'total': t, 'male': m, 'female': f}
        else:
            stack_data = None

        if self.__last_age == rate_pyramid.get_last_age():
            age_data = {}
            for age in range(self.__last_age + 1):
                if is_number(self[age].get_male()) and is_number(rate_pyramid[age].get_male()):
                    m = self[age].get_male() * rate_pyramid[age].get_male()
                else:
                    m = None
                if is_number(self[age].get_female()) and is_number(rate_pyramid[age].get_female()):
                    f = self[age].get_female() * rate_pyramid[age].get_female()
                else:
                    f = None
                if is_number(m) and is_number(f):
                    t = None
                elif is_number(self[age].get_total()) and is_number(rate_pyramid[age].get_total()):
                    t = self[age].get_total() * rate_pyramid[age].get_total()
                else:
                    t = None
                age_data[age] = {'total': t, 'male': m, 'female': f}
        else:
            age_data = None

        delta_pyramid = Pyramid(first_stack_height=self.__first_stack_height, mid_stack_height=self.__mid_stack_height, num_stacks=self.__num_stacks, last_age=self.__last_age,
                                is_total_sum=self.is_total_sum(), is_pos_only=rate_pyramid.is_pos_only(), total=total, male=male, female=female, age_data=age_data, stack_data=stack_data)

        return delta_pyramid

    def calc_rate_vs_female(self, delta_pyramid):
        if is_number(self.get_female()) and self.get_female() != 0:
            if is_number(delta_pyramid.get_total()):
                total = delta_pyramid.get_total() / self.get_female()
            else:
                total = None
            if is_number(delta_pyramid.get_male()):
                male = delta_pyramid.get_male() / self.get_female()
            else:
                male = None
            if is_number(delta_pyramid.get_female()):
                female = delta_pyramid.get_female() / self.get_female()
            else:
                female = None
        else:
            total = None
            male = None
            female = None

        stack_data = {}
        delta_stacks_idx = delta_pyramid.get_stack_in_pyramid_idx()
        for stack_key, stack_idx in delta_stacks_idx.items():
            if stack_key in self.__stacks_idx.keys():
                stack_female = self.children[self.__stacks_idx[stack_key]].get_female()
                if is_number(stack_female) and stack_female != 0:
                    if is_number(delta_pyramid.children[stack_idx].get_total()):
                        t = delta_pyramid.children[stack_idx].get_total() / stack_female
                    else:
                        t = None
                    if is_number(delta_pyramid.children[stack_idx].get_male()):
                        m = delta_pyramid.children[stack_idx].get_male() / stack_female
                    else:
                        m = None
                    if is_number(delta_pyramid.children[stack_idx].get_female()):
                        f = delta_pyramid.children[stack_idx].get_female() / stack_female
                    else:
                        f = None
                else:
                    t = None
                    m = None
                    f = None
            else:
                min_age = int(stack_key[:2])
                if stack_key[2] == '+':
                    max_age = delta_pyramid.get_last_age()
                else:
                    max_age = int(stack_key[2:])
                stack_female_sum = 0
                for age in range(min_age, max_age+1):
                    if is_number(self[age].get_female()):
                        stack_female_sum += self[age].get_female()
                    else:
                        stack_female_sum = None
                        break
                if is_number(stack_female_sum) and stack_female_sum != 0:
                    if is_number(delta_pyramid.children[stack_idx].get_total()):
                        t = delta_pyramid.children[stack_idx].get_total() / stack_female_sum
                    else:
                        t = None
                    if is_number(delta_pyramid.children[stack_idx].get_male()):
                        m = delta_pyramid.children[stack_idx].get_male() / stack_female_sum
                    else:
                        m = None
                    if is_number(delta_pyramid.children[stack_idx].get_female()):
                        f = delta_pyramid.children[stack_idx].get_female() / stack_female_sum
                    else:
                        f = None
                else:
                    t = None
                    m = None
                    f = None

            stack_data[stack_key] = {'total': t, 'male': m, 'female': f}

        age_data = {}
        for age in range(0, delta_pyramid.get_last_age()+1):
            if is_number(delta_pyramid[age].get_total()) and is_number(self[age].get_female()) and self[age].get_female() != 0:
                t = delta_pyramid[age].get_total() / self[age].get_female()
            else:
                t = None
            if is_number(delta_pyramid[age].get_male()) and is_number(self[age].get_female()) and self[age].get_female() != 0:
                m = delta_pyramid[age].get_male() / self[age].get_female()
            else:
                m = None
            if is_number(delta_pyramid[age].get_female()) and is_number(self[age].get_female()) and self[age].get_female() != 0:
                f = delta_pyramid[age].get_female() / self[age].get_female()
            else:
                f = None
            age_data[age] = {'total': t, 'male': m, 'female': f}

        rate_pyramid = Pyramid(first_stack_height=delta_pyramid.get_first_stack_height(), mid_stack_height=delta_pyramid.get_mid_stack_height(), num_stacks=delta_pyramid.get_num_stacks(), last_age=delta_pyramid.get_last_age(),
                               is_total_sum=False, is_pos_only=delta_pyramid.is_pos_only(), total=total, male=male, female=female, age_data=age_data, stack_data=stack_data)
        return rate_pyramid

    def multiply_rate_on_female(self, rate_pyramid):
        if is_number(self.get_female()):
            if is_number(rate_pyramid.get_male()):
                male = self.get_female() * rate_pyramid.get_male()
            else:
                male = None
            if is_number(rate_pyramid.get_female()):
                female = self.get_female() * rate_pyramid.get_female()
            else:
                female = None
            if is_number(male) and is_number(female):
                total = None
            elif is_number(rate_pyramid.get_total()):
                total = self.get_female() * rate_pyramid.get_total()
            else:
                total = None
        else:
            total = None
            male = None
            female = None

        stack_data = {}
        rate_stacks_idx = rate_pyramid.get_stack_in_pyramid_idx()
        for stack_key, stack_idx in rate_stacks_idx.items():
            if stack_key in self.__stacks_idx.keys():
                stack_female = self.children[self.__stacks_idx[stack_key]].get_female()
                if is_number(stack_female):
                    if is_number(rate_pyramid.children[stack_idx].get_male()):
                        m = stack_female * rate_pyramid.children[stack_idx].get_male()
                    else:
                        m = None
                    if is_number(rate_pyramid.children[stack_idx].get_female()):
                        f = stack_female * rate_pyramid.children[stack_idx].get_female()
                    else:
                        f = None
                    if is_number(m) and is_number(f):
                        t = None
                    elif is_number(rate_pyramid.children[stack_idx].get_total()):
                        t = stack_female * rate_pyramid.children[stack_idx].get_total()
                    else:
                        t = None
                else:
                    t = None
                    m = None
                    f = None
            else:
                min_age = int(stack_key[:2])
                if stack_key[2] == '+':
                    max_age = rate_pyramid.get_last_age()
                else:
                    max_age = int(stack_key[2:])
                stack_female_sum = 0
                for age in range(min_age, max_age+1):
                    if is_number(self[age].get_female()):
                        stack_female_sum += self[age].get_female()
                    else:
                        stack_female_sum = None
                        break
                if is_number(stack_female_sum):
                    if is_number(rate_pyramid.children[stack_idx].get_male()):
                        m = stack_female_sum * rate_pyramid.children[stack_idx].get_male()
                    else:
                        m = None
                    if is_number(rate_pyramid.children[stack_idx].get_female()):
                        f = stack_female_sum * rate_pyramid.children[stack_idx].get_female()
                    else:
                        f = None
                    if is_number(m) and is_number(f):
                        t = None
                    elif is_number(rate_pyramid.children[stack_idx].get_total()):
                        t = stack_female_sum * rate_pyramid.children[stack_idx].get_total()
                    else:
                        t = None
                else:
                    t = None
                    m = None
                    f = None

            stack_data[stack_key] = {'total': t, 'male': m, 'female': f}

        age_data = {}
        for age in range(0, rate_pyramid.get_last_age()+1):
            if is_number(self[age].get_female()) and is_number(rate_pyramid[age].get_male()):
                m = self[age].get_female() * rate_pyramid[age].get_male()
            else:
                m = None
            if is_number(self[age].get_female()) and is_number(rate_pyramid[age].get_female()):
                f = self[age].get_female() * rate_pyramid[age].get_female()
            else:
                f = None
            if is_number(m) and is_number(f):
                t = None
            elif is_number(self[age].get_female()) and is_number(rate_pyramid[age].get_total()):
                t = self[age].get_female() * rate_pyramid[age].get_total()
            else:
                t = None
            age_data[age] = {'total': t, 'male': m, 'female': f}

        delta_pyramid = Pyramid(first_stack_height=rate_pyramid.get_first_stack_height(), mid_stack_height=rate_pyramid.get_mid_stack_height(), num_stacks=rate_pyramid.get_num_stacks(), last_age=rate_pyramid.get_last_age(),
                                is_total_sum=self.is_total_sum(), is_pos_only=rate_pyramid.is_pos_only(), total=total, male=male, female=female, age_data=age_data, stack_data=stack_data)
        return delta_pyramid


class Stack(Population):
    def __init__(self, parent=None, min_age=0, max_age=100, is_last_stack=True, is_total_sum=True, is_pos_only=True, fill_zeros=False, total=None, male=None, female=None, age_data=None):
        super().__init__(parent=parent, is_total_sum=is_total_sum, is_pos_only=is_pos_only, fill_zeros=fill_zeros, total=total, male=male, female=female)
        # check the input and declare a class variable with value: min_age
        if not isinstance(min_age, int):
            error_msg = f"min_age '{min_age}' should be an integer."
            raise ValueError(error_msg)
        elif min_age < 0:
            error_msg = f"min_age '{min_age}' should be equal to or greater than 0."
            raise ValueError(error_msg)
        self.__min_age = min_age
        # check the input and declare a class variable with value: max_age
        if not isinstance(max_age, int):
            error_msg = f"max_age '{max_age}' should be an integer."
            raise ValueError(error_msg)
        elif max_age < min_age:
            error_msg = f"max_age '{max_age}' should be equal to or greater than min_age '{min_age}'."
            raise ValueError(error_msg)
        self.__max_age = max_age
        # check the input and declare a class variable with value: is_last_stack
        if not isinstance(is_last_stack, bool):
            error_msg = f"is_last_stack '{is_last_stack}' should be a boolean."
            raise ValueError(error_msg)
        self.__is_last_stack = is_last_stack
        # declare a class variable and insert values with class methods: age_layers
        self.generate_age_layers(fill_zeros=fill_zeros, age_data=age_data)
        return

    def __repr__(self):
        if self.__is_last_stack:
            age_str = f'{self.__min_age:>2}+  '
        else:
            age_str = f'{self.__min_age:>2}-{self.__max_age:>2}'

        total = super().get_total()
        male = super().get_male()
        female = super().get_female()

        if all(isinstance(value, int) for value in [total, male, female]):
            total_str = int_to_str(total, min_print_len)
            male_str = int_to_str(male, min_print_len)
            female_str = int_to_str(female, min_print_len)
        elif is_number(total) and is_number(male) and is_number(female):
            if all(abs(value) <= 1 for value in [total, male, female]):
                total_str = f'{total:8.4%}'
                male_str = f'{male:8.4%}'
                female_str = f'{female:8.4%}'
            else:
                total_str = f'{total:12,.2f}'
                male_str = f'{male:12,.2f}'
                female_str = f'{female:12,.2f}'
        else:
            total_str = str(total)
            male_str = str(male)
            female_str = str(female)

        return f'{age_str}: [ Total: {total_str},    M: {male_str},    F: {female_str} ]'

    def __getitem__(self, age_idx: int):
        return self.children[age_idx]

    def generate_age_layers(self, fill_zeros=False, age_data=None):
        # build layers one by one
        age_layers = []
        for age in range(self.__min_age, self.__max_age + 1):
            if age == self.__max_age:
                is_last_layer = self.__is_last_stack
            else:
                is_last_layer = False

            if fill_zeros is False and age_data is not None:
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

            age_layer = AgeLayer(self, age, is_last_layer=is_last_layer, is_total_sum=super().is_total_sum(), is_pos_only=self.is_pos_only(), fill_zeros=fill_zeros, total=total, male=male, female=female)
            age_layers.append(age_layer)

        self.children = tuple(age_layers)

        if super().is_total_sum():
            super().insert_via_sum_children(insert_parent=False)
        return


class AgeLayer(Population):
    def __init__(self, parent, age: int, is_last_layer=True, is_total_sum=True, is_pos_only=True, fill_zeros=False, total=None, male=None, female=None):
        super().__init__(parent=parent, is_total_sum=is_total_sum, is_pos_only=is_pos_only,fill_zeros=fill_zeros, total=total, male=male, female=female)
        # check the input and declare a class variable with value: age
        if not isinstance(age, int):
            error_msg = f"age '{age}' should be an integer."
            raise ValueError(error_msg)
        elif age < 0:
            error_msg = f"age '{age}' should be equal to or greater than 0."
            raise ValueError(error_msg)
        self.__age = age
        # check the input and declare a class variable with value: is_last_layer
        if not isinstance(is_last_layer, bool):
            error_msg = f"is_last_layer '{is_last_layer}' should be a boolean."
            raise ValueError(error_msg)
        self.__is_last_age = is_last_layer
        return

    def __repr__(self):
        if self.__is_last_age:
            age_str = f'{self.__age:>3}+ '
        else:
            age_str = f'{self.__age:>3}  '

        total = super().get_total()
        male = super().get_male()
        female = super().get_female()

        if all(isinstance(value, int) for value in [total, male, female]):
            total_str = int_to_str(total, min_print_len)
            male_str = int_to_str(male, min_print_len)
            female_str = int_to_str(female, min_print_len)
        elif is_number(total) and is_number(male) and is_number(female):
            if all(abs(value) <= 1 for value in [total, male, female]):
                total_str = f'{total:8.4%}'
                male_str = f'{male:8.4%}'
                female_str = f'{female:8.4%}'
            else:
                total_str = f'{total:12,.2f}'
                male_str = f'{male:12,.2f}'
                female_str = f'{female:12,.2f}'
        else:
            total_str = str(total)
            male_str = str(male)
            female_str = str(female)

        return f'{age_str}: [ Total: {total_str},    M: {male_str},    F: {female_str} ]'


def get_pop_data_from_dict(pop_data: dict):
    total = None
    male = None
    female = None
    if 'total' in pop_data.keys():
        total = pop_data['total']
    if 'male' in pop_data.keys():
        male = pop_data['male']
    if 'female' in pop_data.keys():
        female = pop_data['female']
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
        if input_str == '-':
            output_int = None
        else:
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
