"""Execute the indicator's timer against a bounded fake chart API.

Only MQL array syntax is adapted to C++ vectors. This exercises the production
timer, including its ACK and cleanup decisions; MetaEditor compilation remains
a separate required Windows check.
"""
from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent

HARNESS = r'''
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
using string=std::string;
using ulong=unsigned long;
struct Marker {string id,revision,symbol,timeframe,tool,side;long time;double price;long lineColor;};
enum ENUM_OBJECT_PROPERTY_INTEGER {OBJPROP_TYPE,OBJPROP_COLOR,OBJPROP_WIDTH,OBJPROP_STYLE,OBJPROP_BACK,OBJPROP_TIMEFRAMES,OBJPROP_SELECTABLE,OBJPROP_SELECTED,OBJPROP_HIDDEN};
using ENUM_OBJECT=int;
const int OBJ_HLINE=8,STYLE_SOLID=0,OBJ_ALL_PERIODS=2097151,OBJPROP_PRICE=20,OBJPROP_TOOLTIP=21;
using color=long;
struct Object {double price;int window=0;std::map<int,long> properties;string tooltip;Object(double p=0):price(p){properties[OBJPROP_TYPE]=OBJ_HLINE;}};
struct Chart {long id;string symbol;std::map<string,Object> objects;int period=5;};
const string PREFIX="ISCC_TV_",ACK_FILE="ack",INPUT_FILE="input";
const int FILE_WRITE=1,FILE_TXT=2,FILE_ANSI=4,FILE_SHARE_READ=8,FILE_READ=16,CP_UTF8=65001,INVALID_HANDLE=-1,SYMBOL_DIGITS=1;
std::vector<Marker> input;std::vector<Chart> charts;string ack;int opened=0,writes=0,redraws=0;bool failDraw=false,failRead=false;
ulong nowMs=0,lastAckAt=0;int reads=0,ackWrites=0;bool failDelete=false,failAck=false;
int inputOpens=0,inputLines=0,inputPosition=0;bool failInput=false,manualInput=false;
string overrideInput;std::vector<string> inputRows;
ulong GetTickCount64(){return nowMs;}
template<class T> int ArraySize(const std::vector<T> &a){return a.size();}
template<class T> void ArrayResize(std::vector<T> &a,int n){a.resize(n);}
template<class T> void ArrayCopy(std::vector<T> &a,const std::vector<T> &b){a=b;}
template<class T> void ArrayInitialize(std::vector<T> &a,bool value){std::fill(a.begin(),a.end(),value);}
void check(bool yes,const char* why){if(!yes)throw std::runtime_error(why);}
Chart& get(long id){for(auto& c:charts)if(c.id==id)return c;throw std::runtime_error("Unknown chart");}
long ChartFirst(){return charts.empty()?-1:charts.front().id;}
long ChartNext(long id){for(size_t i=0;i<charts.size();i++)if(charts[i].id==id)return i+1<charts.size()?charts[i+1].id:-1;return -1;}
string ChartSymbol(long id){return get(id).symbol;}
int ChartPeriod(long id){return get(id).period;}
void ChartRedraw(long){++redraws;}
const int PERIOD_M5=5;
long ChartOpen(string symbol,int){++opened;charts.push_back({100+opened,symbol,{}});return charts.back().id;}
int ObjectsTotal(long id){++reads;return get(id).objects.size();}
string ObjectName(long id,int n){++reads;auto p=get(id).objects.begin();std::advance(p,n);return p->first;}
int ObjectFind(long id,string name){++reads;return get(id).objects.count(name)?get(id).objects.at(name).window:-1;}
bool ObjectDelete(long id,string name){++writes;return !failDelete && get(id).objects.erase(name)>0;}
bool ObjectCreate(long id,string name,int type,int window,long,double price){if(failDraw)return false;++writes;auto& o=get(id).objects[name];o.price=price;o.window=window;o.properties[OBJPROP_TYPE]=type;return true;}
bool ObjectGetInteger(long id,string name,ENUM_OBJECT_PROPERTY_INTEGER property,int,long& actual){++reads;if(failRead || !get(id).objects.count(name))return false;actual=get(id).objects.at(name).properties[property];return true;}
long ObjectGetInteger(long id,string name,ENUM_OBJECT_PROPERTY_INTEGER property){long actual=0;ObjectGetInteger(id,name,property,0,actual);return actual;}
bool ObjectGetDouble(long id,string name,int,int,double& actual){++reads;if(failRead || !get(id).objects.count(name))return false;actual=get(id).objects.at(name).price;return true;}
double ObjectGetDouble(long id,string name,int property,int modifier){double actual=0;ObjectGetDouble(id,name,property,modifier,actual);return actual;}
bool ObjectGetString(long id,string name,int,int,string& actual){++reads;if(failRead || !get(id).objects.count(name))return false;actual=get(id).objects.at(name).tooltip;return true;}
bool ObjectSetInteger(long id,string name,ENUM_OBJECT_PROPERTY_INTEGER property,long expected){++writes;if(failDraw)return false;get(id).objects.at(name).properties[property]=expected;return true;}
bool ObjectSetDouble(long id,string name,int,int,double expected){++writes;if(failDraw)return false;get(id).objects.at(name).price=expected;return true;}
bool ObjectSetString(long id,string name,int,string expected){++writes;if(failDraw)return false;get(id).objects.at(name).tooltip=expected;return true;}
double MathAbs(double value){return std::abs(value);}
string TimeToString(long value){return std::to_string(value);}
int StringFind(string a,string b){auto n=a.find(b);return n==string::npos?-1:n;}
int StringLen(string text){return text.size();}
string IntegerToString(long value){return std::to_string(value);}
string DoubleToString(double value,int digits){std::ostringstream out;out<<std::fixed<<std::setprecision(digits)<<value;return out.str();}
long SymbolInfoInteger(string,int){return 2;}
long ToRgb(long rgb){return rgb;}
long FromRgb(long rgb){return rgb;}
long StringToInteger(string value){try{return std::stol(value);}catch(...){return 0;}}
double StringToDouble(string value){try{return std::stod(value);}catch(...){return 0;}}
bool MathIsValidNumber(double value){return std::isfinite(value);}
using datetime=long;
void StringReplace(string& value,string from,string to){size_t p=0;while((p=value.find(from,p))!=string::npos){value.replace(p,from.size(),to);p+=to.size();}}
int StringSplit(string value,char separator,std::vector<string>& parts){parts.clear();std::istringstream in(value);string part;while(std::getline(in,part,separator))parts.push_back(part);return parts.size();}
string snapshotText(){
 std::ostringstream signature;signature<<std::setprecision(17);
 for(const auto& m:input)signature<<m.id<<':'<<m.revision<<':'<<m.symbol<<':'<<m.timeframe<<':'<<m.tool<<':'<<m.side<<':'<<m.time<<':'<<m.price<<':'<<m.lineColor<<';';
 std::ostringstream hash;hash<<std::hex<<std::hash<string>{}(signature.str());string generation=hash.str();generation=string(64-generation.size(),'0')+generation;
 std::ostringstream out;out<<std::setprecision(17)<<"ISCC_CANDLE_LEVELS\t2\t"<<generation<<"\t"<<input.size()<<"\n";
 for(const auto& m:input)out<<m.id<<'\t'<<m.revision<<'\t'<<m.symbol<<'\t'<<m.timeframe<<'\t'<<m.time<<'\t'<<m.price<<'\t'<<m.tool<<'\t'<<m.side<<'\t'<<m.lineColor<<'\n';
 out<<"END\t"<<generation<<'\t'<<input.size()<<'\n';return out.str();
}
int FileOpen(string name,int,int,int){if(name==INPUT_FILE){++inputOpens;if(failInput)return INVALID_HANDLE;inputRows.clear();inputPosition=0;std::istringstream in(manualInput?overrideInput:snapshotText());string line;while(std::getline(in,line))inputRows.push_back(line);return 2;}if(failAck)return INVALID_HANDLE;++ackWrites;lastAckAt=nowMs;ack.clear();return 1;}
bool FileIsEnding(int){return inputPosition>=inputRows.size();}
string FileReadString(int){++inputLines;return FileIsEnding(2)?"":inputRows[inputPosition++];}
int FileWriteString(int,string text){ack+=text;return text.size();}
void FileClose(int){}
@@PRODUCTION@@
void tick(ulong elapsed=1000){nowMs+=elapsed;OnTimer();}
Marker marker(char id,string symbol="USTEC"){return {string(24,id),"1",symbol,"M5","ray","high",1234,29547.45,16711680};}
int count(string text,string token){int n=0;size_t p=0;while((p=text.find(token,p))!=string::npos){++n;p+=token.size();}return n;}
int main(int argc,char**argv){try{
 string scenario=argv[1];
 if(scenario=="missing"){
  charts={{1,"SPX500",{{"manual",77}}}};input={marker('a'),marker('b'),marker('c')};
  for(int i=0;i<20;i++)tick();
  check(opened==0 && charts.size()==1,"A missing symbol must not open charts, including repeated markers/timers");
  check(count(ack,"\tpending\t")==3,"Missing chart must remain pending");
  check(get(1).objects.count("manual"),"Manual object was removed");
 }else if(scenario=="existing"){
  charts={{1,"USTEC",{{"manual",77}}},{2,"USTEC",{}},{3,"SPX500",{}}};
  input={marker('a'),marker('b'),marker('c'),marker('d',"SPX500")};
  for(int i=0;i<20;i++)tick();
  check(opened==0 && charts.size()==3,"Existing charts were duplicated");
  check(get(1).objects.size()==4 && get(2).objects.size()==3 && get(3).objects.size()==1,"Levels must draw on all matching existing charts");
  check(count(ack,"\tready\t")==4,"Matching charts must be acknowledged");
 }else if(scenario=="reopen"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();charts.clear();
  for(int i=0;i<3;i++)tick();
  check(charts.empty() && count(ack,"\tpending\t")==1,"Closing a chart must not reopen it");
  charts.push_back({9,"USTEC",{{"manual",77}}});tick();
  check(opened==0 && get(9).objects.size()==2 && count(ack,"\tready\t")==1,"Manually reopened chart must receive the level");
 }else if(scenario=="delete"){
  charts={{1,"USTEC",{{"manual",77},{PREFIX+string(24,'a'),99}}}};
  tick();check(charts.size()==1 && get(1).objects.size()==1 && get(1).objects.count("manual"),"Cleanup must remove only owned stale objects");
 }else if(scenario=="failed-draw"){
  charts={{1,"USTEC",{}}};input={marker('a')};failDraw=true;tick();
  check(count(ack,"\tpending\t")==1 && opened==0,"Failed drawing must not acknowledge or open a replacement chart");
 }else if(scenario=="steady"){
  charts={{1,"USTEC",{}},{2,"SPX500",{}}};input={marker('a'),marker('b'),marker('c',"SPX500")};tick();
  writes=redraws=0;for(int i=0;i<20;i++)tick();
  check(writes==0 && redraws==0,"Unchanged levels must not rewrite objects or force chart redraws");
  check(count(ack,"\tready\t")==3,"Unchanged levels must still be verified and acknowledged");
 }else if(scenario=="batch"){
  charts={{1,"USTEC",{}},{2,"USTEC",{}},{3,"SPX500",{}}};
  for(int i=0;i<20;i++)input.push_back(marker('a'+i));tick();
  check(redraws==2,"A whole marker batch must redraw each changed chart only once");
  writes=redraws=0;input[0].price+=1;input[0].lineColor=123;tick();
  check(writes==4 && redraws==2,"Only changed price/color on matching charts should be written");
 }else if(scenario=="repair"){
  charts={{1,"USTEC",{{"manual",77}}}};input={marker('a')};tick();
  auto name=PREFIX+input[0].id;get(1).objects.erase(name);tick(2000);
  check(get(1).objects.count(name) && get(1).objects.count("manual"),"Deleted owned level must be restored without touching manual objects");
  get(1).objects.at(name).window=1;tick(2000);
  check(get(1).objects.at(name).window==0,"Owned object in an indicator subwindow must be repaired");
  get(1).objects.at(name).properties[OBJPROP_COLOR]=321;tick(2000);
  check(get(1).objects.at(name).properties[OBJPROP_COLOR]==input[0].lineColor,"Actual color drift must be repaired");
 }else if(scenario=="read-error"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();failRead=true;writes=redraws=0;tick(2000);
  check(count(ack,"\tpending\t")==1 && writes==0 && redraws==0,"A read error must not reuse a cached ready ACK");
  failRead=false;tick();check(count(ack,"\tready\t")==1,"Reading recovery must resume confirmation");
 }else if(scenario=="skip"){
  charts={{1,"USTEC",{}},{2,"SPX500",{}}};input={marker('a')};tick();
  reads=ackWrites=0;auto priorAck=ack;auto priorTime=lastAckAt;tick();
  check(reads==0 && ackWrites==0 && ack==priorAck && lastAckAt==priorTime,"Idle timer must not query objects or refresh an unverified ACK");
  tick();check(reads>0 && ackWrites==1 && lastAckAt==nowMs,"Actual verification must renew the ACK before its five-second expiry");
 }else if(scenario=="input-change"){
  charts={{1,"USTEC",{{"manual",77}}}};input={marker('a'),marker('b')};tick();
  input[0].price+=1;input.erase(input.begin()+1);reads=0;tick();
  check(reads>0 && get(1).objects.at(PREFIX+input[0].id).price==input[0].price,"A new snapshot must apply on the next timer, including between scheduled audits");
  check(get(1).objects.size()==2 && get(1).objects.count("manual"),"Snapshot deletion must clean owned objects immediately and preserve manual objects");
  input[0].lineColor=123;tick();check(get(1).objects.at(PREFIX+input[0].id).properties[OBJPROP_COLOR]==123,"A color-only snapshot change must bypass the idle cache");
 }else if(scenario=="topology"){
  charts={{1,"USTEC",{}},{2,"SPX500",{}}};input={marker('a')};tick();
  get(1).symbol="SPX500";tick();check(get(1).objects.empty() && count(ack,"\tpending\t")==1,"Symbol change must clean the old destination without a stale ready ACK");
  charts.push_back({9,"USTEC",{}});tick();check(get(9).objects.size()==1 && count(ack,"\tready\t")==1,"A manually opened chart must bypass the cache");
  get(9).objects.clear();get(9).period=60;tick();check(get(9).objects.size()==1,"Period change must trigger immediate verification");
 }else if(scenario=="cleanup"){
  charts={{1,"USTEC",{{"manual",77}}},{2,"USDCHF",{{"manual",88}}}};input={marker('a')};tick();
  get(2).objects[PREFIX+string(24,'z')]=99;
  for(int i=0;i<31;i++)tick();
  check(get(2).objects.size()==1 && get(2).objects.count("manual"),"Periodic sweeping must remove orphaned owned objects, including unrelated symbols, while preserving manual ones");
 }else if(scenario=="cleanup-failure"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();auto priorAck=ack;auto priorTime=lastAckAt;
  input.clear();failDelete=true;tick();
  check(ack==priorAck && lastAckAt==priorTime && get(1).objects.size()==1,"Failed deletion must not confirm the new snapshot");
  failDelete=false;tick();check(get(1).objects.empty() && count(ack,"\tready\t")==0 && lastAckAt==nowMs,"Failed cleanup must retry on the next timer");
 }else if(scenario=="ack-failure"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();auto priorTime=lastAckAt;failAck=true;tick(2000);
  check(lastAckAt==priorTime,"A failed ACK write must not refresh readiness");
  failAck=false;reads=0;tick();check(reads>0 && lastAckAt==nowMs,"ACK failure must force a fresh verification on the next timer");
 }else if(scenario=="cost"){
  charts={{1,"USTEC",{}},{2,"SPX500",{}},{3,"USDCHF",{}},{4,"USDCNH",{}},{5,"AUDUSD",{}},{6,"USDSGD",{}}};
  for(int i=0;i<19;i++)input.push_back(marker('a'+i,i<10?"USTEC":"SPX500"));
  for(int c=1;c<=6;c++)for(int i=0;i<50;i++)get(c).objects["manual_"+std::to_string(i)]=77;
  tick();reads=ackWrites=writes=redraws=0;
  for(int i=0;i<60;i++){tick();check(nowMs-lastAckAt<5000,"Steady-state ACK must stay within the receiver's freshness window");}
  check(writes==0 && redraws==0 && count(ack,"\tready\t")==19,"Cost sample must preserve all levels without chart writes");
  std::cout<<"reads="<<reads<<" ackWrites="<<ackWrites<<" inputLines="<<inputLines<<" inputOpens="<<inputOpens<<" ";
 }else if(scenario=="broken-input"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();auto priorAck=ack;auto priorTime=lastAckAt;
  manualInput=true;overrideInput="";tick();
  check(ack==priorAck && lastAckAt==priorTime,"Broken header cannot refresh a cached ACK");
  overrideInput=snapshotText();StringReplace(overrideInput,"END\t","BAD\t");
  for(int i=0;i<6;i++)tick();
  check(lastAckAt==priorTime && nowMs-lastAckAt>5000,"A damaged body must expire the last ACK instead of keeping cached readiness alive");
  manualInput=false;tick();check(lastAckAt==nowMs && count(ack,"\tready\t")==1,"Valid input must recover after cache invalidation");
 }else if(scenario=="cached-body"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();auto priorAck=ack;auto priorTime=lastAckAt;
  manualInput=true;overrideInput=snapshotText();StringReplace(overrideInput,"END\t","BAD\t");inputLines=0;tick();
  check(inputLines==1 && ack==priorAck && lastAckAt==priorTime,"A header-only turn must not publish an ACK");
  inputLines=0;tick();check(inputLines>1 && ack==priorAck && lastAckAt==priorTime,"Every actual audit must validate the body even with an unchanged header");
 }else if(scenario=="cache-empty"){
  charts={{1,"USTEC",{{"manual",77}}}};input={marker('a'),marker('b')};tick();input.clear();tick();tick();
  check(get(1).objects.size()==1 && count(ack,"\tready\t")==0,"Cached rows must shrink after removing every level");
  input={marker('c')};tick();tick();check(get(1).objects.size()==2 && count(ack,"\tready\t")==1,"A smaller replacement must not retain old cached rows");
 }else if(scenario=="topology-input"){
  charts={{1,"USTEC",{}}};input={marker('a')};tick();inputLines=0;get(1).period=60;tick();
  check(inputLines==3 && lastAckAt==nowMs,"A topology change must bypass the input cache and validate the entire snapshot");
 }else throw std::runtime_error("Unknown scenario");
 std::cout<<scenario<<" PASS\n";return 0;
 }catch(const std::exception& error){std::cerr<<error.what()<<"\n";return 1;}}
'''


@unittest.skipUnless(shutil.which('c++'), 'C++ compiler unavailable for the MQL timer harness')
class ChartPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        source = Path(os.environ.get('CANDLE_INDICATOR_SOURCE', ROOT/'mt5/ISCC Candle Levels.mq5')).read_text()
        cls.binary = cls.compile_source(source, Path(cls.temporary.name)/'timer.cpp')

    @staticmethod
    def compile_source(source, file):
        start = source.index('const ulong VERIFY_INTERVAL_MS') if 'const ulong VERIFY_INTERVAL_MS' in source else source.index('bool Contains(')
        drawing = source[start:source.index('int OnInit()')]
        timer = source[source.index('void OnTimer()'):]
        loader_end=source.index('// ACK files') if '// ACK files' in source else source.index('bool Contains(')
        loader = source[source.index('bool ReadLine('):loader_end]
        production = (loader + drawing + timer).replace('Marker &markers[]', 'std::vector<Marker> &markers')
        production = re.sub(r'Marker (\w+)\[\];',r'std::vector<Marker> \1;',production)
        production = production.replace('string parts[];', 'std::vector<string> parts;')
        production = production.replace('string &fields[]', 'std::vector<string> &fields')
        production = production.replace('long charts[];string symbols[];bool changed[];', 'std::vector<long> charts;std::vector<string> symbols;std::vector<bool> changed;')
        production = production.replace('bool found[],ready[];', 'std::vector<bool> found,ready;')
        harness = HARNESS.replace('ISCC_TV_', 'ISCC_ADDON_TV_') if 'PREFIX="ISCC_ADDON_TV_"' in source else HARNESS
        file.write_text(harness.replace('@@PRODUCTION@@', production))
        binary = str(file.with_suffix(''))
        result = subprocess.run([shutil.which('c++'), '-std=c++17', str(file), '-o', binary], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return binary

    def scenario(self, name):
        result = subprocess.run([self.binary, name], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_symbol_never_opens_charts_on_repeated_updates(self): self.scenario('missing')
    def test_existing_charts_receive_levels_without_duplicates(self): self.scenario('existing')
    def test_closed_chart_stays_closed_and_manual_reopening_resumes_drawing(self): self.scenario('reopen')
    def test_deletion_preserves_manual_objects(self): self.scenario('delete')
    def test_failed_drawing_does_not_open_replacement_chart(self): self.scenario('failed-draw')
    def test_unchanged_levels_have_no_writes_or_forced_redraws(self): self.scenario('steady')
    def test_redraws_are_batched_per_changed_chart(self): self.scenario('batch')
    def test_deleted_or_edited_owned_objects_are_repaired(self): self.scenario('repair')
    def test_read_failure_is_pending_and_recovers_without_cached_ack(self): self.scenario('read-error')
    def test_idle_turn_does_not_poll_objects_or_refresh_cached_ack(self): self.scenario('skip')
    def test_changed_snapshot_applies_without_waiting_for_the_periodic_audit(self): self.scenario('input-change')
    def test_chart_symbol_period_and_manual_opening_bypass_the_cache(self): self.scenario('topology')
    def test_orphaned_objects_are_periodically_removed_from_unrelated_charts(self): self.scenario('cleanup')
    def test_failed_cleanup_never_acknowledges_the_new_snapshot(self): self.scenario('cleanup-failure')
    def test_ack_failure_requires_fresh_verification_on_retry(self): self.scenario('ack-failure')
    def test_long_idle_session_preserves_levels_and_fresh_ack(self): self.scenario('cost')
    def test_invalid_header_or_body_expires_ack_and_recovery_revalidates(self): self.scenario('broken-input')
    def test_cached_body_is_never_used_to_publish_a_fresh_ack(self): self.scenario('cached-body')
    def test_empty_or_smaller_snapshot_drops_cached_rows(self): self.scenario('cache-empty')
    def test_topology_change_requires_a_full_input_read(self): self.scenario('topology-input')

    def test_input_cache_reduces_rows_read_without_reducing_object_checks_or_ack_frequency(self):
        previous=(ROOT/'tests/fixtures/candle-indicator-1.16.mq5').read_text()
        baseline=self.compile_source(previous,Path(self.temporary.name)/'before-cache.cpp')
        results=[]
        for binary in (baseline,self.binary):
            result=subprocess.run([binary,'cost'],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,0,result.stderr)
            results.append({key:int(value) for key,value in re.findall(r'(reads|ackWrites|inputLines|inputOpens)=(\d+)',result.stdout)})
        self.assertLess(results[1]['inputLines'],results[0]['inputLines']*.6,results)
        for key in ('reads','ackWrites','inputOpens'):self.assertEqual(results[1][key],results[0][key],results)

    def test_idle_chart_queries_drop_compared_with_the_real_previous_indicator(self):
        previous = (ROOT/'tests/fixtures/candle-indicator-1.15.mq5').read_text()
        baseline = self.compile_source(previous, Path(self.temporary.name)/'before.cpp')
        results = []
        for binary in (baseline, self.binary):
            result = subprocess.run([binary, 'cost'], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            results.append({key:int(value) for key,value in re.findall(r'(reads|ackWrites)=(\d+)',result.stdout)})
        self.assertLessEqual(results[1]['reads'], results[0]['reads']*.6, results)
        self.assertLessEqual(results[1]['ackWrites'], results[0]['ackWrites']/2, results)


if __name__ == '__main__':
    unittest.main()
