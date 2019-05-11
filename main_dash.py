import warnings
import sched
import time

from helper_functions.blob import uploadBlob
from preprocessing.preprocess import preprocess

warnings.filterwarnings('ignore')

s = sched.scheduler(time.time, time.sleep)
account_name = 'halalanaidata'
account_key = 'BglYcnBSLXLgXKEeY4dGu9bsYw7QKBeuJnAWD4EsGAXaUi+7uQn+Ltf8Hf+DEOj/OCUPIPcAWkEV0gtrqILQcQ=='
container = 'processed'

def to_run(num):
	local_paths = preprocess()
	uploadBlob(account_name, account_key, container, local_paths)
	print('Hi!', num)
def sched_run():
	s.enter(900, 1, to_run, ('1',))
	s.enter(1800, 1, to_run, ('2',))
	s.enter(2700, 1, to_run, ('3',))
	s.enter(3600, 1, to_run, ('3',))
	s.enter(4500, 1, to_run, ('3',))
	s.run()
if __name__ == '__main__':
	sched_run()