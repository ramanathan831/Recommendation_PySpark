import sys
import findspark
findspark.init()
from pyspark import SparkContext

support_threshold = 100

def create_1d_list(curr_line):
	if(curr_line is not None):
		listvar = []
		index = 0
		curr_line_split = curr_line.split(" ")
		while(index < len(curr_line_split)):
			listvar.append([curr_line_split[index], 1])
			index += 1
		return listvar

def compute_frequency(curr_line):
	if(curr_line is not None):
		return [curr_line[0], sum(curr_line[1])]

def drop_items_based_on_thresh(curr_line):
	if(curr_line is not None):
		if(curr_line[1] >= support_threshold):
			return curr_line

def give_combinations(list_of_products, num_elts_in_combination):
	if(num_elts_in_combination == 0):
		return [[]]
	listvar = []
	for index in range(0,len(list_of_products)):
		curr_product_id = list_of_products[index]
		remaining_list = list_of_products[index+1:]
		for inner_index in give_combinations(remaining_list, num_elts_in_combination-1):
			listvar.append([curr_product_id] + inner_index)
	return listvar

def create_2d_list(keys):
	def _create_2d_list(curr_line):
		if(curr_line is not None):
			sortedinp = curr_line.split(" ")
			sortedinp.sort()
			listvar = give_combinations(sortedinp,2)
			two_d_list = []
			index = 0
			while(index < len(listvar)):
				if(listvar[index][0] in keys and listvar[index][1] in keys):
					two_d_list.append((tuple(listvar[index]),1))
				index += 1
			return two_d_list
	return _create_2d_list

def get_two_d_association_rules(one_d_data):
	def _get_two_d_association_rules(curr_line):
		listvar = []
		listvar.append(((curr_line[0][0], curr_line[0][1]), float(curr_line[1])/one_d_data[curr_line[0][0]]))
		listvar.append(((curr_line[0][1], curr_line[0][0]), float(curr_line[1])/one_d_data[curr_line[0][1]]))
		return listvar
	return _get_two_d_association_rules

def create_3d_list(keys):
	def _create_3d_list(curr_line):
		if(curr_line is not None):
			sortedinp = curr_line.split(" ")
			sortedinp.sort()
			listvar = give_combinations(sortedinp,3)
			three_d_list = []
			index = 0
			listvar.sort()
			while(index < len(listvar)):
				sublist = ((listvar[index][0],listvar[index][1]), (listvar[index][0],listvar[index][2]),(listvar[index][1],listvar[index][2]))
				if(sublist[0] in keys and sublist[1] in keys and sublist[2] in keys):
					three_d_list.append((tuple(listvar[index]),1))
				index += 1
			return three_d_list
	return _create_3d_list

def get_three_d_association_rules(two_d_data):
	def _get_three_d_association_rules(curr_line):
		listvar = []
		listvar.append(((curr_line[0][0], curr_line[0][1], curr_line[0][2]), float(curr_line[1])/two_d_data[(curr_line[0][0],curr_line[0][1])]))
		listvar.append(((curr_line[0][1], curr_line[0][2], curr_line[0][0]), float(curr_line[1])/two_d_data[(curr_line[0][1],curr_line[0][2])]))
		listvar.append(((curr_line[0][0], curr_line[0][2], curr_line[0][1]), float(curr_line[1])/two_d_data[(curr_line[0][0],curr_line[0][2])]))
		return listvar
	return _get_three_d_association_rules

def main():
	datasetfile = sys.argv[1]
	opfile = sys.argv[2]
	sparkcontext = SparkContext("local", "Product Recommendation")
	data = sparkcontext.textFile(datasetfile)

	product_list = data.flatMap(create_1d_list)
	frequency_list = product_list.reduceByKey(lambda elt1, elt2: elt1+elt2)
	one_d_frequent = frequency_list.map(drop_items_based_on_thresh)
	one_d_frequent = one_d_frequent.filter(bool)
	one_d_map = one_d_frequent.collectAsMap()
	keys = one_d_frequent.keys()
	keys = list(keys.collect())

	two_d_itemsets = data.flatMap(create_2d_list(keys)).reduceByKey(lambda elt1, elt2: elt1+elt2)
	two_d_frequent = two_d_itemsets.map(drop_items_based_on_thresh)
	two_d_frequent = two_d_frequent.filter(bool)
	two_d_recommendations = two_d_frequent.sortByKey(ascending=True)
	two_d_recommendations_sorted = two_d_recommendations.sortBy(lambda elt:-elt[1])
	two_d_map = two_d_recommendations_sorted.collectAsMap()
	keys = two_d_recommendations_sorted.keys()
	keys = list(keys.collect())

	two_d_association_rules = two_d_recommendations_sorted.flatMap(get_two_d_association_rules(one_d_map))
	two_d_association_rules_sorted = two_d_association_rules.sortByKey(ascending=True)
	two_d_association_rules_sorted = two_d_association_rules_sorted.sortBy(lambda elt:-elt[1])

	opfile_ptr = open(opfile, "w")
	print("Top Itemsets of Size 2")
	opfile_ptr.write("Top Itemsets of Size 2\n")
	opfile_ptr.flush()
	index = 0
	for value in two_d_association_rules_sorted.collect():
		print(value)
		opfile_ptr.write("%s -> %s with confidence %f\n" %(value[0][0], value[0][1], value[1]))
		opfile_ptr.flush()
		index += 1
		if(index > 4):
			break

	three_d_itemsets = data.flatMap(create_3d_list(keys)).reduceByKey(lambda elt1, elt2: elt1+elt2)
	three_d_frequent = three_d_itemsets.map(drop_items_based_on_thresh)
	three_d_frequent = three_d_frequent.filter(bool)
	three_d_recommendations = three_d_frequent.sortByKey(ascending=True)
	three_d_recommendations_sorted = three_d_recommendations.sortBy(lambda elt:-elt[1])
	three_d_map = three_d_recommendations_sorted.collectAsMap()

	three_d_association_rules = three_d_recommendations_sorted.flatMap(get_three_d_association_rules(two_d_map))
	three_d_association_rules_sorted = three_d_association_rules.sortByKey(ascending=True)
	three_d_association_rules_sorted = three_d_association_rules_sorted.sortBy(lambda elt:-elt[1])

	print("\nTop Itemsets of Size 3")
	opfile_ptr.write("\nTop Itemsets of Size 3\n")
	opfile_ptr.flush()
	index = 0
	for value in three_d_association_rules_sorted.collect():
		print(value)
		opfile_ptr.write("%s,%s -> %s with confidence %f\n" %(value[0][0], value[0][1], value[0][2], value[1]))
		opfile_ptr.flush()
		index += 1
		if(index > 4):
			break
	opfile_ptr.close()

if __name__ == '__main__':
	main()
