proc = GB('StreamReader')
proc.foreach(lambda x: execute('SADD', 'myset', x['id']))
proc.register('mystream')