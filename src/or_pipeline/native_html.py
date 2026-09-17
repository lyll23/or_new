"""Conservative model-day extraction from a bound React query object.

Adapted from a previously independently checked same-object evidence parser.
Source SHA256: 9a96ef379ca2c012f0abada307b9ccbb75eb87a90d9af4b825a38b90fab2d02b
No JavaScript is executed; unsupported representations stay explicit issues.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass,field
from decimal import Decimal
import datetime as dt,email.utils,hashlib,json,re

UTC=dt.timezone.utc
DEC=json.JSONDecoder(parse_float=Decimal)
PUSH=re.compile(r'self\.__next_f\.push\(\[1,("(?:\\.|[^"\\])*")\]\)',re.S)
REF=re.compile(r'\$@?([a-f0-9]+)\Z')
LOC=re.compile(r'rsc\.offset\[(\d+)\]\.(analytics|model_chart|modelChart|activityTimeSeries)\[(\d+|"(?:\\.|[^"\\])*")\](?:\.rsc_ref\[([a-f0-9]+)\])?\Z')
VERSION='native_html_same_object_v1'
CORE=('count','total_prompt_tokens','total_completion_tokens')

def compact(x):return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False,default=lambda v: {'__decimal_evidence__':str(v)} if isinstance(v,Decimal) else (_ for _ in ()).throw(TypeError(type(v).__name__)))
def fingerprint(x):return hashlib.sha256(compact(x).encode('utf-8')).hexdigest()
def iso(x):return x.isoformat(timespec='milliseconds').replace('+00:00','Z') if x else None
def utc(raw):
    x=dt.datetime.fromisoformat(raw.replace('Z','+00:00'))
    if x.tzinfo is None:raise ValueError('timezone_missing')
    return x.astimezone(UTC)

class BindingError(ValueError):pass

@dataclass
class Node:
    start:int
    end:int
    path:tuple
    value:object
    children:dict=field(default_factory=dict)
    duplicate_keys:list=field(default_factory=list)

class Document:
    def __init__(self,html):
        self.text=''.join(json.loads(m[1]) for m in PUSH.finditer(html))
        self.nodes=[];self.by_start={};self.frames=defaultdict(list);self.warnings=[]
        if not self.text:raise BindingError('unsupported_non_rsc_or_no_old_compatible_stream')
        # Offsets use exactly the frozen extractor's decoded concatenation.
        # JSON and length-prefixed text are consumed whole before another frame
        # is sought, so a fake frame identifier inside text cannot be referenced.
        pos=0
        while pos<len(self.text):
            m=re.search(r'(?:^|\n)([a-f0-9]+):',self.text[pos:])
            if not m:break
            start=pos+m.end();fid=m[1]
            if self.text[start:start+1] in '[{"':
                try:
                    node,end=self.parse(start,(fid,start));self.frames[fid].append(node);pos=end
                except (ValueError,IndexError) as e:
                    self.warnings.append({'frame_id':fid,'start':start,'reason':str(e)})
                    pos=self.text.find('\n',start)
                    if pos<0:break
            elif self.text[start:start+1]=='T':
                t=re.match(r'T([a-f0-9]+),',self.text[start:])
                if not t:raise BindingError('unsupported_text_frame_length')
                left=int(t[1],16);pos=start+t.end()
                while left>0 and pos<len(self.text):
                    left-=len(self.text[pos].encode('utf-8'));pos+=1
                if left!=0:raise BindingError('truncated_or_invalid_text_frame')
            else:
                # Non-JSON resource/control records are not data references.
                pos=self.text.find('\n',start)
                if pos<0:break
        self.text_sha256=hashlib.sha256(self.text.encode('utf-8')).hexdigest()

    def ws(self,p):
        while p<len(self.text) and self.text[p] in ' \r\n\t':p+=1
        return p

    def parse(self,p,path):
        p=self.ws(p);start=p;ch=self.text[p];children={};dups=[]
        if ch=='{':
            value={};p=self.ws(p+1)
            while self.text[p]!='}':
                key,end=DEC.raw_decode(self.text,p)
                if not isinstance(key,str):raise BindingError('nonstring_json_key')
                p=self.ws(end)
                if self.text[p]!=':':raise BindingError('missing_json_colon')
                child,p=self.parse(p+1,path+(key,))
                if key in children:dups.append(key)
                children.setdefault(key,[]).append(child);value[key]=child.value;p=self.ws(p)
                if self.text[p]=='}':break
                if self.text[p]!=',':raise BindingError('missing_json_comma')
                p=self.ws(p+1)
            p+=1
        elif ch=='[':
            value=[];p=self.ws(p+1)
            while self.text[p]!=']':
                i=len(value);child,p=self.parse(p,path+(i,));children[i]=[child];value.append(child.value);p=self.ws(p)
                if self.text[p]==']':break
                if self.text[p]!=',':raise BindingError('missing_json_comma')
                p=self.ws(p+1)
            p+=1
        else:value,p=DEC.raw_decode(self.text,p)
        node=Node(start,p,path,value,children,dups)
        self.nodes.append(node);self.by_start[start]=node
        return node,p

    def label(self,n):
        return f'rsc.frame[{n.path[0]}]@character[{n.path[1]}]'+''.join('['+json.dumps(x,ensure_ascii=False)+']' for x in n.path[2:])

    def child(self,n,key):
        found=n.children.get(key,[])
        if len(found)!=1:raise BindingError('missing_or_duplicate_key:'+str(key))
        return found[0]

    def resolve(self,n):
        chain=[];seen=set()
        while isinstance(n.value,str) and REF.fullmatch(n.value):
            fid=REF.fullmatch(n.value)[1]
            if fid in seen:raise BindingError('reference_cycle')
            seen.add(fid);targets=self.frames.get(fid,[])
            if len(targets)!=1:raise BindingError('missing_or_duplicate_reference_frame:'+fid)
            target=targets[0]
            chain.append({'reference_locator':self.label(n),'reference_raw':n.value,'target_locator':self.label(target),'target_start':target.start})
            n=target
        return n,chain

    def could_reach(self,n,target,seen=None):
        """Conservative ambiguity check only; never used to establish binding."""
        seen=set() if seen is None else set(seen)
        if n.start in seen:return False
        seen.add(n.start)
        if n.start<=target<n.end:return True
        if isinstance(n.value,str) and REF.fullmatch(n.value):
            return any(self.could_reach(t,target,seen) for t in self.frames.get(REF.fullmatch(n.value)[1],[]))
        return any(self.could_reach(c,target,seen) for values in n.children.values() for c in values)

    def bind(self,row):
        m=LOC.fullmatch(str(row.get('source_locator','')))
        if not m:raise BindingError('unsupported_old_locator')
        offset=int(m[1]);key=m[2];idx=json.loads(m[3]);arr=self.by_start.get(offset)
        if arr is None or not isinstance(arr.value,(dict,list)):raise BindingError('exact_array_offset_not_found')
        native=self.child(arr,idx)
        if any(n.duplicate_keys for n in self.nodes if native.start<=n.start<native.end):
            raise BindingError('duplicate_native_row_json_key')
        if fingerprint(native.value)!=fingerprint(row.get('raw_fields')):raise BindingError('whole_native_raw_fields_mismatch')
        if isinstance(arr.value,dict):raise BindingError('dictionary_daily_scope_requires_review')
        if m[4]:
            targets=self.frames.get(m[4],[])
            if len(targets)!=1 or targets[0].start!=offset:raise BindingError('old_array_reference_not_unique_exact_target')
        owners=[];related_errors=[]
        for n in self.nodes:
            if not isinstance(n.value,dict) or key not in n.children:continue
            try:
                resolved,chain=self.resolve(self.child(n,key))
                if resolved.start==offset:owners.append((n,chain))
            except BindingError as e:
                # Errors matter when this was the old explicitly named ref.
                if any(self.could_reach(c,offset) for c in n.children[key]):related_errors.append(str(e))
        if related_errors:raise BindingError('ambiguous_referenced_owner:'+','.join(related_errors))
        if len(owners)!=1:raise BindingError('statistics_owner_count:'+str(len(owners)))
        owner,arrchain=owners[0]
        if owner.duplicate_keys:raise BindingError('duplicate_statistics_object_key')
        cache=self.child(owner,'cachedAt')
        # Require query.state.data to resolve to this exact object. An enclosing
        # query-like component or adjacent query is never enough.
        queries=[];ambiguous_query_refs=[]
        for q in self.nodes:
            if not isinstance(q.value,dict) or 'queryKey' not in q.children:continue
            try:
                state,sc=self.resolve(self.child(q,'state'))
                data,dc=self.resolve(self.child(state,'data'))
                if data.start==owner.start:queries.append((q,sc+dc))
            except BindingError as e:
                for child in self.nodes:
                    if q.start<=child.start<q.end and isinstance(child.value,str) and REF.fullmatch(child.value):
                        targets=self.frames.get(REF.fullmatch(child.value)[1],[])
                        if any(self.could_reach(t,owner.start) for t in targets):ambiguous_query_refs.append(str(e))
                continue
        if ambiguous_query_refs:raise BindingError('ambiguous_query_reference_to_statistics_owner')
        if len(queries)!=1:raise BindingError('exact_parent_query_count:'+str(len(queries)))
        query,qchain=queries[0]
        if query.duplicate_keys:raise BindingError('duplicate_query_key')
        qk,kchain=self.resolve(self.child(query,'queryKey'))
        if not isinstance(qk.value,list) or len(qk.value)!=3:raise BindingError('query_key_shape_unconfirmed')
        q0,c0=self.resolve(self.child(qk,0));q1,c1=self.resolve(self.child(qk,1));qp,pc=self.resolve(self.child(qk,2))
        if q0.value!='model-page' or q1.value!='appStats' or not isinstance(qp.value,dict):raise BindingError('query_scope_not_model_page_appStats')
        if qp.duplicate_keys:raise BindingError('duplicate_query_identity_key')
        perma=self.child(qp,'permaslug').value;variant=self.child(qp,'variant').value
        raw=row['raw_fields']
        if not isinstance(perma,str) or not perma or not isinstance(variant,str) or not variant:raise BindingError('explicit_query_identity_missing')
        if raw.get('model_permaslug')!=perma or row.get('model_permaslug')!=perma:raise BindingError('query_native_candidate_permaslug_mismatch')
        if raw.get('variant')!=variant or row.get('variant_raw')!=variant:raise BindingError('query_native_candidate_variant_mismatch_or_missing')
        if raw.get('date')!=row.get('date_raw'):raise BindingError('raw_date_vs_candidate_mismatch')
        try:date=dt.datetime.fromisoformat(raw['date'])
        except (ValueError,TypeError,KeyError):raise BindingError('raw_date_unparseable')
        if date.date().isoformat()!=row.get('business_date') or date.time()!=dt.time():raise BindingError('business_day_or_midnight_label_mismatch')
        for k in CORE:
            if type(raw.get(k)) is not int or raw[k]<0 or raw[k]!=row.get(k):raise BindingError('native_core_exact_integer_unconfirmed:'+k)
        return {'status':'exact_same_statistics_object_bound','source_locator_old':row['source_locator'],
            'decoded_rsc_stream_sha256':self.text_sha256,'native_row_locator':self.label(native),
            'array_locator':self.label(arr),'array_character_offset':offset,'statistics_property_name':key,
            'statistics_object_locator':self.label(owner),'statistics_object_keys':list(owner.value),
            'cachedAt_locator':self.label(cache),'cachedAt_raw':cache.value,
            'query_locator':self.label(query),'queryKey_raw':query.value['queryKey'],
            'query_identity_resolved':{'permaslug':perma,'variant':variant},
            'queryHash_raw':query.value.get('queryHash'),
            'reference_evidence':arrchain+qchain+kchain+c0+c1+pc,
            'full_native_raw_fields_sha256':fingerprint(native.value),
            'raw_values_identity_and_date_unchanged':True,
            'business_date_time_evidence':{
                'business_date_label_raw':raw['date'],
                'native_date_has_explicit_timezone':date.tzinfo is not None,
                'native_date_timezone_name':date.tzname() if date.tzinfo is not None else None,
                'business_day_timezone_status':'timezone_unconfirmed',
                'business_day_timezone_verified_by_original':False,
                'day_end_quality_screen_rule':'inherited_frozen_API_rule:UTC_midnight_after_business_date_label',
                'evidence_boundary':'UTC cachedAt/capture does not establish the business-day label timezone. No rankings-daily endpoint definition transferred.'},
            'document_parse_warnings':self.warnings,
            'duplicate_frame_ids':[k for k,v in self.frames.items() if len(v)>1]}


DAILY_KEYS = frozenset(('analytics', 'model_chart', 'modelChart', 'activityTimeSeries'))

def extract_model_daily(body: str, content_type: str = '') -> dict:
    """Return only full native rows with an exact appStats owner binding.

    This establishes provenance, not the semantics of every counter or the
    platform's historical day-label timezone. No current-day row is erased.
    """
    issues = []
    if 'text/x-component' in content_type or ('self.__next_f.push' not in body and re.match(r'^[a-f0-9]+:', body)):
        body = '<script>self.__next_f.push([1,' + json.dumps(body, ensure_ascii=False) + '])</script>'
    try:
        document = Document(body)
    except (BindingError, ValueError, IndexError, RecursionError) as exc:
        return {'rows': [], 'issues': ['unsupported_flight_document:' + str(exc)]}
    if document.warnings:
        return {'rows': [], 'issues': ['flight_parse_warnings_require_review'], 'parse_warnings': document.warnings}
    if any(len(values) != 1 for values in document.frames.values()):
        return {'rows': [], 'issues': ['duplicate_flight_frame_ids_require_review']}
    rows, seen = [], set()
    for owner in document.nodes:
        if not isinstance(owner.value, dict):
            continue
        for key in DAILY_KEYS.intersection(owner.children):
            try:
                array, _ = document.resolve(document.child(owner, key))
            except BindingError as exc:
                issues.append('unbound_statistics_array:' + str(exc))
                continue
            if not isinstance(array.value, list):
                issues.append('unsupported_daily_container:' + key)
                continue
            for i, raw in enumerate(array.value):
                if not isinstance(raw, dict):
                    continue
                identity = (array.start, i)
                if identity in seen:
                    continue
                seen.add(identity)
                if not any(name in raw for name in CORE):
                    continue
                date_raw = raw.get('date')
                candidate = {
                    'source_locator': f'rsc.offset[{array.start}].{key}[{i}]',
                    'raw_fields': raw,
                    'model_permaslug': raw.get('model_permaslug'),
                    'variant_raw': raw.get('variant'),
                    'date_raw': date_raw,
                    'business_date': date_raw[:10] if isinstance(date_raw, str) else None,
                    **{name: raw.get(name) for name in CORE},
                }
                try:
                    evidence = document.bind(candidate)
                except (BindingError, ValueError, TypeError, OverflowError) as exc:
                    issues.append(candidate['source_locator'] + ':' + str(exc))
                    continue
                rows.append({'raw': raw, 'locator': evidence['native_row_locator'], 'evidence': evidence})
    if not rows and not issues:
        issues.append('no_exact_bound_model_daily_rows')
    return {'rows': rows, 'issues': issues}
