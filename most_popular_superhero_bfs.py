# Accumulator Concept
from pyspark import SparkConf, SparkContext

startCharacterID = 5306 # Spiderman
targetCharacterID = 14 # Adam

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    
    color = 'White'
    distance = 9999
    
    if heroID == startCharacterID:
        color = 'Gray'
        distance = 0
        
    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/Marvel-graph.txt')
    return inputFile.map(convertToBFS)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    
    results = []

    if color == 'Gray':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance+1
            newColor = 'Gray'
            
            if targetCharacterID == connection:
                hitCounter.add(1)
            
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        
        color = 'Black' 
    
    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    
    distance1 = data1[1]
    distance2 = data2[1]
    
    color1 = data1[2]
    color2 = data2[2]
    
    edges = []
    distance = 9999
    color = 'White'
    
    if len(edges1) > 0:
        edges = edges1
    elif len(edges2) > 0:
        edges = edges2
        
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2
        
    if color1 == 'White' and (color2 == 'Gray' or color2 == 'Black'):
        color = color2
    
    if color2 == 'Gray' and color2 == 'Black':
        color = color2
    
    return (edges, distance, color)
    

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

hitCounter = sc.accumulator(0)

iterationRdd = createStartingRdd()
print(iterationRdd.take(5))

for iteration in range(10):
    print('Running BFS iteration: ' + str(iteration+1))
    mapped = iterationRdd.flatMap(bfsMap)
    
    print('Processing ' + str(mapped.count()) + ' values.')
    
    if hitCounter.value > 0:
        print('Hit target character from ' + str(hitCounter.value)
        + ' different direction(s).')
        
        break
    
    iterationRdd = mapped.reduceByKey(bfsReduce)
    
