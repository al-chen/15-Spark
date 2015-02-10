from pyspark import SparkContext
import Sliding, argparse

def flat_map(value):
    # Only call Sliding.children if value is on "frontier" (most recent level)
    if value[1] == level:
        list_of_children = Sliding.children(WIDTH, HEIGHT, value[0])
        list_of_children.append(value) # append parent
        return list_of_children
    return [value]

def bfs_map(value):
    """ YOUR CODE HERE """
    # Check to see if value is parent (and thus already has a level)
    if type(value[0]) == tuple:
        return value
    # Give level to children
    return (value, level + 1)

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    # Always return lower level
    if value1 < value2:
        return value1
    return value2

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)


    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    # Initialize RDD to contain starting solution position
    myRDD = sc.parallelize([(sol, 0)])

    prevCount = 1

    k = 16
    half = k / 2
    while True:
        myRDD = myRDD.flatMap(flat_map).map(bfs_map).reduceByKey(bfs_reduce)

        # Check to see if no new positions are being added every other MapReduce iteration
        if (level % 2 != 0):
            currCount = myRDD.count()
            if currCount == prevCount:
                break
            prevCount = currCount

        # Parition into k groups every half iterations
        if (level != 0) and (level % half == 0):
            myRDD = myRDD.partitionBy(k)

        level += 1

    """ YOUR OUTPUT CODE HERE """
    myList = sorted(myRDD.collect(), key=lambda x: (x[1], x[0]))
    for item in myList:
        output(str(item[1]) + " " + str(item[0]))

    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
