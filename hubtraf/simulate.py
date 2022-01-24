import asyncio
import structlog
import argparse
import numpy as np
import random
import time
import socket
import os
import pwd
import grp
import sys
import secrets
from hubtraf.user import User
from hubtraf.auth.dummy import login_dummy
from functools import partial
from collections import Counter

def delay_array(users, reps):
    """
    Function delay_array:
    This function creates a string of integer values of length 'users' that 
    is used as a wait value before spawning the jupyterhub single user server for  
    that user.
    Input:
     users = the number of users you wish to add to the system 
     reps = how many users you wish to assign each second 
            (typically fails above 4)
    Output:
     array_int = string of length users with integer values of the second 
                 a user is to be added
    """
    
    # determine the starting point as a float that when truncated becomes 
    # the first integer second
    start = 1.001
    
    # set the increment by which to add each element so that there will 
    # be 'reps' numbers between whole integers
    inc = 0.99999/reps
    stop = start + users * inc
    array_float = np.arange(start,stop,inc)
    
    # create a string of evenly spaced float numbers that assign users to 
    # a specific second when truncated to the nearest int value 
    array_int = np.arange(start,stop,inc).astype(int)
    
    return  array_int
    
array = delay_array(100,1)
delaylist = delay_array(100,1)

#array1, array = delay_array(100,2)
#array2, array = delay_array(100,2)
#array3, array = delay_array(100,3)
#array4, array = delay_array(100,4)
#array5, array = delay_array(100,5)
#array6, array = delay_array(100,6)

async def no_auth(*args, **kwargs):
    return True

async def simulate_user(hub_url, username, api_token, delay_seconds, code_execute_seconds, final_batch):
    await asyncio.sleep(delay_seconds)
    print("final_batch= ",final_batch)
    async with User(username, hub_url, no_auth) as u:
        try:
            if not await u.ensure_server_api(api_token):
               return 'start-server'
            if not await u.start_kernel():
                return 'start-kernel'
            if not await u.assert_code_output("5 * 4", "20", 5, code_execute_seconds):
                return 'run-code'
        except:
            print("something went wrong")

        finally:
            if ((u.state == User.States.KERNEL_STARTED) and (final_batch == True)) :
                 #await u.stop_kernel()
                 #await u.stop_server()
                 print("would normally stop server here")

async def run(args):
    # FIXME: Pass in individual arguments, not argparse object
    api_token = os.environ['JUPYTERHUB_API_TOKEN']
    awaits  = [[] for i in range(10)]
    count = 0
    final_batch = False
    delaycount = 0

    for i in range(0,args.user_count,100): 
        print("range =", i, " to ", i+100)
        if i < args.user_count:  
          for j in range(i,i+100):
            print("j= ", j )
            print("delaycount= ", delaycount )
            delay = int(delaylist[delaycount])
            print("delay =", delay)
            print ("count =", count)
            if ((i+100) == args.user_count): 
               final_batch = True
            awaits[count].append(simulate_user(
                args.hub_url,
                f'{args.user_prefix}' + str(j),
                api_token,
                delay,
                60,
                final_batch 
            ))
            delaycount +=1 
               
        count += 1
        delaycount = 0
        print("count = ",count)
    outputs0 = await asyncio.gather(*awaits[0])
    outputs1 = await asyncio.gather(*awaits[1])
    outputs2 = await asyncio.gather(*awaits[2])
    outputs3 = await asyncio.gather(*awaits[3])
    outputs4 = await asyncio.gather(*awaits[4])
    outputs5 = await asyncio.gather(*awaits[5])
    outputs6 = await asyncio.gather(*awaits[6])
    print(Counter(outputs0))
    print(Counter(outputs1))
    print(Counter(outputs2))
    print(Counter(outputs3))
    print(Counter(outputs4))
    print(Counter(outputs6))

def main():
    # create the parser
    my_parser = argparse.ArgumentParser()

    # add the arguments
    my_parser.add_argument(
        'hub_url',
        help='Hub URL to send traffic to (without a trailing /)'
    )
    my_parser.add_argument(
        'user_count',
        type=int,
        help='Number of users to simulate'
    )
    my_parser.add_argument(
        '--user-prefix',
        default=socket.gethostname(),
        help='Prefix to use when generating user names'
    )
    my_parser.add_argument(
        '--user-session-min-runtime',
        default=60,
        type=int,
        help='Min seconds user is active for'
    )
    my_parser.add_argument(
        '--user-session-max-runtime',
        default=300,
        type=int,
        help='Max seconds user is active for'
    )

    # parse the input arguments and get a Namespace object that contains the user input
    args = my_parser.parse_args()

    processors=[structlog.processors.TimeStamper(fmt="ISO")]

    #if args.json:
    #    processors.append(structlog.processors.JSONRenderer())
    #else:
    #   processors.append(structlog.dev.ConsoleRenderer())
    processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(processors=processors)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(args))


if __name__ == '__main__':
    main()
