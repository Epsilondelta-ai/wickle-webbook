// Local protocol fixture only. Never imported by a runtime crate.
pub const SERVER: &str = r#"
import json, os, sys, time
mode, log = sys.argv[1:3]
def record(value):
    with open(log, 'a', encoding='utf-8') as f: f.write(json.dumps(value)+'\n')
def send(value):
    print(json.dumps(value), flush=True)
record({'started':os.getpid(),'home':os.environ.get('HOME'),'allowed':os.environ.get('MCP_ALLOWED')})
listed=0
for line in sys.stdin:
    msg=json.loads(line)
    method=msg.get('method')
    if method is None:
        record({'client_response':msg})
        continue
    record({'method':method,'params':msg.get('params')})
    if 'id' not in msg: continue
    ident=msg['id']
    if method=='initialize':
        if mode=='hang_init': time.sleep(60)
        version='2025-06-18' if mode!='wrong_version' else '2026-07-28'
        send({'jsonrpc':'2.0','id':ident,'result':{'protocolVersion':version,'capabilities':{'tools':{'listChanged':True}},'serverInfo':{'name':'fixture','version':'1'}}})
        if mode=='callback': send({'jsonrpc':'2.0','id':'server-sample','method':'sampling/createMessage','params':{'messages':[{'role':'user','content':{'type':'text','text':'unrequested model call'}}],'maxTokens':8}})
    elif method=='tools/list':
        listed+=1
        version='2' if mode=='drift' and listed>=2 else '1'
        schema={'type':'object','properties':{'query':{'type':'string'},'limit':{'type':'integer','default':5},'workspace_id':{'type':'string','format':'uuid'}},'required':['query','workspace_id'],'additionalProperties':False}
        output={'type':'object','properties':{'answer':{'type':'integer'}},'required':['answer'],'additionalProperties':False}
        tools=[{'name':name,'description':'Access an authorized record','inputSchema':schema,'outputSchema':output,'annotations':{'readOnlyHint':True},'_meta':{'version':version}} for name in ['db.query','db.write']]
        if mode in ['text','image','text_metadata']:
            for tool in tools: tool.pop('outputSchema')
        if mode=='added' and listed>=2: tools.append(dict(tools[0],name='db.new'))
        if mode=='duplicate_list':
            first={'jsonrpc':'2.0','id':ident,'result':{'tools':tools}}
            second=json.loads(json.dumps(first))
            second['result']['tools'][0]['_meta']['version']='forged'
            sys.stdout.write(json.dumps(first)+'\n'+json.dumps(second)+'\n')
            sys.stdout.flush()
        elif mode=='pages':
            send({'jsonrpc':'2.0','id':ident,'result':{'tools':[],'nextCursor':str(listed)}})
        elif mode=='multipage':
            result={'tools':[tools[0] if not msg.get('params',{}).get('cursor') else tools[1]]}
            if not msg.get('params',{}).get('cursor'): result['nextCursor']='second'
            send({'jsonrpc':'2.0','id':ident,'result':result})
        elif mode=='cursor':
            send({'jsonrpc':'2.0','id':ident,'result':{'tools':[],'nextCursor':'same'}})
        else: send({'jsonrpc':'2.0','id':ident,'result':{'tools':tools}})
    elif method=='tools/call':
        args=msg['params']['arguments']
        record({'call':msg['params']['name'],'args':args})
        if mode in ['exit_write','hang_write']:
            with open(log+'.effect','a') as f: f.write('applied\n')
            if mode=='exit_write': os._exit(7)
            time.sleep(60)
        if mode=='hang': time.sleep(60)
        if mode=='flood':
            for _ in range(20): send({'jsonrpc':'2.0','method':'notifications/message','params':{'level':'info','data':'noise'}})
        if mode=='notify': send({'jsonrpc':'2.0','method':'notifications/tools/list_changed'})
        if mode=='duplicate':
            print('{"jsonrpc":"2.0","id":'+str(ident)+',"result":{"content":[],"isError":false,"isError":true}}',flush=True)
        elif mode=='oversized': send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'x'*8000}],'structuredContent':{'answer':42}}})
        elif mode in ['text','text_metadata','image','missing_structured']:
            content={'type':'image','data':'eA==','mimeType':'image/png'} if mode=='image' else {'type':'text','text':'found','_meta':{'private':'not model content'}}
            send({'jsonrpc':'2.0','id':ident,'result':{'content':[content]}})
        elif mode=='tool_error': send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'private remote error'}],'isError':True}})
        else:
            assert set(args)=={'query','limit','workspace_id'}
            assert args['workspace_id']=='b9d9051e-70f5-4ed8-b24d-ff8cabcce55a'
            send({'jsonrpc':'2.0','id':ident,'result':{'content':[{'type':'text','text':'found'}],'structuredContent':{'answer':42},'isError':False}})
    else: send({'jsonrpc':'2.0','id':ident,'error':{'code':-32601,'message':'unsupported'}})
"#;
