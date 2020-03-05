import sys
import findspark
findspark.init()
from pyspark import SparkContext

def give_combinations(list_of_friends, num_elts_in_combination):
	if(num_elts_in_combination == 0):
		return [[]]
	listvar = []
	for index in range(0,len(list_of_friends)):
		curr_friend_id = list_of_friends[index]
		remaining_list = list_of_friends[index+1:]
		for inner_index in give_combinations(remaining_list, num_elts_in_combination-1):
			listvar.append([curr_friend_id] + inner_index)
	return listvar

def generate_pairs_for_each_person(curr_line):
	person_id = curr_line[0]
	corresponding_friend_list = curr_line[1]
	list_of_pairs = []
	index = 0
	while(index < len(corresponding_friend_list)):
		individual_friend_id = corresponding_friend_list[index]
		if(person_id < individual_friend_id):
			key = (person_id, individual_friend_id)
		else:
			key = (individual_friend_id, person_id)
		list_of_pairs.append((key, 0))
		index += 1
	index = 0
	pair_combinations = give_combinations(corresponding_friend_list, 2)
	while(index < len(pair_combinations)):
		pair = pair_combinations[index]
		combination_id_1 = pair[0]
		combination_id_2 = pair[1]
		if(combination_id_1 < combination_id_2):
			key = (combination_id_1, combination_id_2)
		else:
			key = (combination_id_2, combination_id_1)
		list_of_pairs.append((key, 1))
		index += 1
	return list_of_pairs

def make_key_value_pair(curr_line):
	person_id = int(curr_line.split()[0])
	if(len(curr_line.split("\t")) > 1 and curr_line.split("\t")[1][0].isdigit()):
		friend_list = [int(x) for x in curr_line.split("\t")[1].split(',')]
	else:
		friend_list = []
	return person_id, friend_list

def remove_existing_friend_pairs(curr_line):
	if(curr_line is not None):
		if not (0 in list(curr_line[1])):
			if(1 in list(curr_line[1])):
				return curr_line

def get_mutual_friend_count(curr_line):
	if(curr_line is not None):
		return [curr_line[0], sum(curr_line[1])]

def suggest_friends(curr_line):
	if(curr_line is not None):
		person_id_1 = curr_line[0][0]
		person_id_2 = curr_line[0][1]
		common_friend_count = curr_line[1]
		pair1 = (person_id_1, (person_id_2, common_friend_count))
		pair2 = (person_id_2, (person_id_1, common_friend_count))
		return [pair1, pair2]

def eliminate_low_count_recommendations(curr_line):
	if(curr_line is not None):
		return [curr_line[0], list(curr_line[1])[:10]]

def main():
	datasetfile = sys.argv[1]
	opfile = sys.argv[2]
	user_ids = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]
	sparkcontext = SparkContext("local", "Friend Recommendation")
	data = sparkcontext.textFile(datasetfile)
	friend_ownership = data.map(make_key_value_pair)
	friend_edges = friend_ownership.flatMap(generate_pairs_for_each_person)
	groupbykey = friend_edges.groupByKey()
	cleaned_group = groupbykey.map(remove_existing_friend_pairs)
	cleaned_group = cleaned_group.filter(bool)
	summed_groups = cleaned_group.map(get_mutual_friend_count)
	possible_recommendations = summed_groups.flatMap(suggest_friends)
	possible_recommendations_sorted = possible_recommendations.sortBy(lambda elt:-elt[1][1])
	possible_recommendations_sorted = possible_recommendations_sorted.sortByKey(ascending=True)
	restrict_recommendations = possible_recommendations_sorted
	groupbykey = restrict_recommendations.groupByKey()
	final_recommendations = groupbykey.map(eliminate_low_count_recommendations)

	opfile_ptr = open(opfile,"w")
	for value in final_recommendations.collect():
		if(value[0] in user_ids):
			opfile_ptr.write("%d\t" % (value[0]))
			inner_index = 0
			while(inner_index < len(value[1])):
				print(value[1][inner_index][0], value[1][inner_index], value[1], value)
				opfile_ptr.write("%d," % (value[1][inner_index][0]))
				inner_index+= 1
			opfile_ptr.write("\n")
	opfile_ptr.close()

if __name__ == '__main__':
	main()
