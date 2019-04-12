import json, constant, collections,re,sys,time
from mpi4py import MPI

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map                         #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global counter
global comm,size,rank
rigions = {}


def main():
    global comm,size,rank,counter,rigions
    init()

    twitter_num = 0
    to_be_scattered = [None]*size


    with open(sys.argv[1],"r") as fh:
        if rank == 0:#master
            # count = 0
            to_be_scattered = []
            line = fh.readline()#remove first line
            line = fh.readline()
            while line:
                if line != ']}\n':#remove last line
                    to_be_scattered.append(line)
                line = fh.readline()
                if len(to_be_scattered) == size:
                    comm.scatter(to_be_scattered, root = 0)
                    to_be_scattered = []

            #last several twitters (% size) are not scattered. handled by master itself
            for each in to_be_scattered:
                deal_with_twitter(each)
            comm.scatter(['all sent']*size, root = 0)

        else:#slave
            message = comm.scatter(to_be_scattered, root = 0)
            while message != 'all sent':
                deal_with_twitter(message)
                # print(rank,'        :     ',message)
                message = comm.scatter(to_be_scattered, root = 0)

        comm.barrier()
        combined_data = comm.gather([rigions,counter], root = 0)

        if rank == 0 :
            combine(combined_data)#including master itself's data

            print(counter)
            for area in rigions.keys():
                print('Top 5 of ',area,' are: ',rigions[area].most_common(5))

def combine(combined_data):
    global comm,size,rank,counter,rigions

    for slave_seq in range(1, size):
        counter.update(combined_data[slave_seq][1])
        for rigion_name in rigions.keys():
            rigions[rigion_name].update(combined_data[slave_seq][0][rigion_name])


def deal_with_twitter(twitter):#twitter is json
    global comm,size,rank,counter,rigions
    while twitter[-1] != '}':#deal with massive tail
        twitter = twitter[:-1]
    twitter = json.loads(twitter)

    location = locate(get_coordinate(twitter))
    tags = get_tags(get_text(twitter))#a list of tags
    counter[location] += 1
    for tag in tags:
        rigions[location][tag]+=1


def init():
    global comm,size,rank,counter,rigions
    comm = MPI.COMM_WORLD
    size = comm.size
    rank = comm.rank
    load_map()
    counter = collections.Counter()

def load_map(filename = constant.MELB_GRID):
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
        global min_x, max_x, min_y, max_y
        global scale_x, scale_y
        global coordinates_map
        global regions

        min_x = 180
        max_x = 0
        min_y = 0
        max_y = -90
        scale_x = None
        scale_y = None
        coordinates_map = {}
        for feature in data['features']:
            rigions[feature['properties']['id']] = collections.Counter()#build a seperate Counter for each rigion
            p = feature['properties']
            xmin = p['xmin']
            xmax = p['xmax']
            ymin = p['ymin']
            ymax = p['ymax']
            min_x = min(min_x, xmin)
            max_x = max(max_x, xmax)
            min_y = min(min_y, ymin)
            max_y = max(max_y, ymax)
            if scale_x == None and scale_y == None:
                scale_x = round(xmax-xmin, 2)
                scale_y = round(ymax-ymin, 2)
            coordinates_map[p['id']] = [xmin, xmax, ymin, ymax]

def locate(coordinate):
    x, y = coordinate
    dy = int((max_y-y)/scale_y)
    if float(dy) == (max_y-y)/scale_y:
        dy -= 1

    dx = int((x-min_x)/scale_x + 1)
    if float(dx) == (max_y-y)/scale_y:
        dx -= 1

    if dy in [0,1,2]:
        if dx < 1:
            dx = 1
    elif dy == 3:
        if dx < 3:
            dx = 3

    if dx in [0,1,2,3]:
        if dy < 0:
            dy = 0
    elif dx == 4:
        if dy > 2:
            dy = 2

    first = constant.ALPHABET[dy]
    grid_cell = first+str(dx)
    return grid_cell

# def load_dataset(filename):
#     with open(filename, 'rb') as input_file:
#         data = json.load(input_file)
#         return data['rows']
#


def get_coordinate(twitter):
    return  twitter['value']['geometry']['coordinates']

def get_text(twitter):
    return twitter['value']['properties']['text']

def get_tags(text):
    return re.findall(' \#[^# ]* ',text)

start_time = time.time()
main()
if rank == 0 :
    print('Total_time: ', time.time() - start_time)
