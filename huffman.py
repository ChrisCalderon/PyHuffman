import multiprocessing as mp
from threading import Thread
from collections import Counter, namedtuple

ALL_DONE = "all done"
Node = namedtuple("Node", ["child1", "child2", "weight", "symbol", "code"])

def _sort_func(node):
    return node.weight

def _encode_proc(proc_number, work_queue, result_queue, message_queue):
    while True:
        #get a node from the work queue
        node = work_queue.get()
        #if it is an end node, add the symbol-code pair to the result queue
        if node == ALL_DONE:
            break
        elif node.child1 == node.child2 == None:
            message_queue.put("Symbol processed! : proc%d" % proc_number)
            result_queue.put({node.symbol:node.code})
        #otherwise do some work and add some nodes to the work queue
        else:
            message_queue.put("More work to be done! : proc%d" % proc_number)
            node.child1.code.extend(node.code)
            node.child1.code.append(48)
            node.child2.code.extend(node.code)
            node.child2.code.append(49)
            work_queue.put(node.child1)
            work_queue.put(node.child2)

def _reporter_thread(message_queue):
    while True:
        message = message_queue.get()
        if message == ALL_DONE:
            break
        else:
            print message

def _encode_tree(tree, symbol_count):
    """Uses multiple processes to walk the tree and build the huffman codes."""
    #Create a manager to manage the queues, and a pool of workers.
    manager = mp.Manager()
    worker_pool = mp.Pool()
    #create the queues you will be using
    work = manager.Queue()
    results = manager.Queue()
    messages = manager.Queue()
    #add work to the work queue, and start the message printing thread
    work.put(tree)
    message_thread = Thread(target=_reporter_thread, args=(messages,))
    message_thread.start()
    #add the workers to the pool and close it
    for i in range(mp.cpu_count()):
        worker_pool.apply_async(_encode_proc, (i, work, results, messages))
    worker_pool.close()
    #get the results from the results queue, and update the table of codes
    table = {}
    for i in range(1, symbol_count + 1):
        table.update(results.get())
        print "Symbols to process: %d" % (symbol_count - i)
    #tell all the processes to stop
    for i in range(mp.cpu_count()):
        work.put(ALL_DONE)
    #wait for the processes to end
    worker_pool.join()
    #tell the reporter thread to stop
    messages.put(ALL_DONE)
    message_thread.join()
    #wait for it to stop
    return table

def make_huffman_table(data):
    """
    data is an iterable containing the string that needs to be encoded.
    Returns a dictionary mapping symbols to codes.
    """
    #Build a list of Nodes out of the characters in data
    nodes = [Node(None, None, weight, symbol, bytearray()) for symbol, weight in Counter(data).items()]
    nodes.sort(reverse=True, key=_sort_func)
    symbols = len(nodes)
    append_node = nodes.append
    while len(nodes) > 1:
        #make a new node out of the two nodes with the lowest weight and add it to the list of nodes.
        child2, child1 = nodes.pop(), nodes.pop()
        new_node = Node(child1, child2, child1.weight+child2.weight, None, bytearray())
        append_node(new_node)
        #then resort the nodes
        nodes.sort(reverse=True, key=_sort_func)
    top_node = nodes[0]
    return _encode_tree(top_node, symbols)

def chars(fname):
    """
    A simple generator to make reading from files without loading them 
    totally into memory a simple task.
    """
    f = open(fname)
    char = f.read(1)
    while char != '':
        yield char
        char = f.read(1)
    f.close()
    raise StopIteration

if __name__ == "__main__":
    import pprint
    #import urllib2
    import subprocess
    import os.path
    f = "romeo-and-juliet.txt"
    if not os.path.isfile(f):
        subprocess.call("curl http://www.gutenberg.org/dirs/etext98/2ws1610.txt >> %s" % f, shell=True)
    text = chars(f)
    table = make_huffman_table(text)
    print "Done with table generation!"
    pprint.pprint(sorted(table.items(), key=lambda (key, val): len(val)))
    print len(table)
