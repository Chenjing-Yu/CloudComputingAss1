import json, constant, collections,re,sys,time
from mpi4py import MPI

global min_x, max_x, min_y, max_y    #the rectangle range of the melbourne grid cells
global scale_x, scale_y              #the "width" and "height" of a grid cell on the map
global coordinates_map                         #key is the area id (eg 'A1'), value is [xmin, xmax, ymin, ymax]
global counter,map
global comm,size,rank
rigions = {}

def main():
    global comm,size,rank,counter,rigions

    init()
    to_be_scattered = [None]*size

    if size > 1:#slaves exist
        if rank == 0:
            with open(sys.argv[1],"r") as fh:
                to_be_scattered = [0]
                line = fh.readline()#remove first line
                line = fh.readline()
                while line:
                    to_be_scattered.append(line)
                    line = fh.readline()
                    if len(to_be_scattered) == size:
                        comm.scatter(to_be_scattered, root = 0)#master do nothing
                        to_be_scattered = [0]

                #last several twitters (% size) are not scattered. handled by master itself
                for each in to_be_scattered:
                    if each != 0:
                        deal_with_twitter(each)
                comm.scatter(['all sent']*size, root = 0)

        else:#slave
            message = comm.scatter(to_be_scattered, root = 0)
            while message != 'all sent':
                deal_with_twitter(message)
                message = comm.scatter(to_be_scattered, root = 0)
        comm.barrier()

    else:#no slave
        with open(sys.argv[1],"r") as fh:
            line = fh.readline()#remove first line
            line = fh.readline()
            while line:
                deal_with_twitter(line)
                line = fh.readline()

    combined_data = comm.gather([rigions,counter], root = 0)
    if rank == 0 :
        combine(combined_data)#including master itself's data

        gen_results()

def combine(combined_data):
    global comm,size,rank,counter,rigions

    for slave_seq in range(1, size):
        counter.update(combined_data[slave_seq][1])
        for rigion_name in rigions.keys():
            rigions[rigion_name].update(combined_data[slave_seq][0][rigion_name])

def deal_with_twitter(twitter):#twitter is json
    global comm,size,rank,counter,rigions
    if not re.search('id', twitter):
        return

    while twitter[-1] != '}':#deal with massive tail
        twitter = twitter[:-1]
    twitter = json.loads(twitter)
    location = locate(get_coordinate(twitter))
    if location is None:#ignore twitters who are not from these areas
        return
    tags = get_tags(get_text(twitter))#a list of tags
    counter[location] += 1
    for tag in tags:
        rigions[location][tag]+=1

def init():
    global comm,size,rank,counter,rigions,map
    comm = MPI.COMM_WORLD
    size = comm.size
    rank = comm.rank
    load_map()
    counter = collections.Counter()
    map = sys.argv[1]

def load_map(filename = constant.MELB_GRID):
    global min_x, max_x, min_y, max_y, scale_x, scale_y, coordinates_map, regions
    min_x = 180
    max_x = 0
    min_y = 0
    max_y = -90
    scale_x = None
    scale_y = None
    with open(filename, 'rb') as input_file:
        data = json.load(input_file)
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

def locate(coordinates):
    x, y = coordinates
    if min_x <= x <= max_x and min_y <= y <= max_y:
        if y == max_y:
            i = 0
        else:
            i = int(round((max_y-min_y), 2)/scale_y)-int(round((y-min_y), 2)/scale_y)-1
        if i < len(constant.ALPHABET):
            if x == min_x:
                second = 1
            else:
                second = int(round((max_x-min_x), 2)/scale_x)-int(round((max_x-x), 2)/scale_x)
            first = constant.ALPHABET[i]
            # deal with d3 left edge and c5 upper edge
            if x == 145 and first == 'D':
                second = 2
            if second == 5 and y == -37.8:
                first = 'C'  # constant.ALPHABET[i+1]

            grid_cell = first+str(second)
            if grid_cell in coordinates_map:
                return grid_cell
    return None

def get_coordinate(twitter):
    try:
        return twitter['doc']['coordinates']['coordinates']
    except:
        try:
            reverse = twitter['doc']['geo']['coordinates']
            return [reverse[1], reverse[0]]
        except:
            return [200, 200]


def get_text(twitter):
    return twitter['doc']['text']


def get_tags(text):
    words = text.split(' ')
    hashtags = []
    for word in words:
        if len(word) > 1 and word[0] == '#':
            hashtags.append(word.lower())
    return hashtags

def gen_results():
    for grid_cell, post_number in counter.most_common():
        unique = 0
        count = 0
        prev = None
        for tag, num in regions[grid_cell].most_common():
            if num != prev:
                unique += 1
                prev = num
                if unique > 5:
                    break
            count += 1
        print('{}: {} posts. {}'.format(grid_cell, post_number, str(regions[grid_cell].most_common(count))))

start_time = time.time()
main()
if rank == 0 :
    print('Total_time: ', time.time() - start_time)
