proc = GB('StreamReader', desc='MyStreamReader')
proc.foreach(lambda x: execute('SADD', 'myset', x['id']))
proc.register('mystream')