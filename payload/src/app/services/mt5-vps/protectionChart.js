// Local price preview only. Trading commands remain in the management form.
const NS='http://www.w3.org/2000/svg';
const WIDTH=440,HEIGHT=138,TOP=14,BOTTOM=124;
const positive=value=>Number.isFinite(Number(value)) && Number(value)>0;
function snapPrice(value,step) {
  if(!positive(value) || !positive(step))return null;
  const price=Number((Math.round(Number(value)/Number(step))*Number(step)).toFixed(10));
  return positive(price)?price:null;
}
function chartScale(values,step) {
  const prices=values.filter(positive).map(Number);
  if(!prices.length)return null;
  const low=Math.min(...prices),high=Math.max(...prices);
  const padding=Math.max((high-low)*.15,positive(step)?Number(step)*10:0,high*.0001);
  return {low:Math.max(0,low-padding),high:high+padding};
}
function minuteScale(model) {
  const prices=[];
  for(const row of model.bars || []){
    if(Array.isArray(row) && positive(row[2]) && positive(row[3]) && Number(row[2])>=Number(row[3]))prices.push(Number(row[2]),Number(row[3]));
  }
  if(!prices.length)return null;
  if(positive(model.current))prices.push(Number(model.current));
  const low=Math.min(...prices),high=Math.max(...prices);
  const span=Math.max(high-low,positive(model.tickSize)?Number(model.tickSize)*20:0,high*1e-7);
  const center=(high+low)/2,padding=span*.15;
  return {low:Math.max(0,center-span/2-padding),high:center+span/2+padding};
}
function protectionScale(model,selected='sl',zoom=1,framing='auto') {
  let base=selected==='sl' && framing!=='level'?minuteScale(model):null;
  if(!base){
    const entry=Number(positive(model.entry)?model.entry:model.current),tick=Number(model.tickSize);
    if(!positive(entry))return chartScale([model.sl,model.tp],tick);
    const stop=positive(model.sl)?Math.abs(Number(model.sl)-entry):0;
    const take=positive(model[selected==='partialTp'?'partialTp':'tp'])?Math.abs(Number(model[selected==='partialTp'?'partialTp':'tp'])-entry):0;
    const distance=Math.max(stop,positive(tick)?tick*20:0,entry*.0001);
    const prices=[entry,model.current,model.sl];
    for(const row of model.bars || [])prices.push(row[2],row[3]);
    if(selected==='tp' || selected==='partialTp'){
      const direction=model.side==='sell'?-1:1;
      prices.push(model.tp,model.partialTp,Math.max(positive(tick)?tick:1e-10,entry+direction*Math.max(take*4,distance*12)));
    } else prices.push(Math.max(positive(tick)?tick:1e-10,entry-distance*2),entry+distance*2);
    base=chartScale(prices,tick);
  }
  if(!base)return null;
  const factor=Number.isFinite(zoom)?Math.max(1/64,Math.min(1024,zoom)):1;
  const span=(base.high-base.low)*factor,center=(base.high+base.low)/2;
  const low=Math.max(0,center-span/2);
  return {low,high:low+span};
}
function priceAt(y,scale,step) {
  if(!scale || !Number.isFinite(y) || !(scale.high>scale.low))return null;
  const ratio=Math.max(0,Math.min(1,(y-TOP)/(BOTTOM-TOP)));
  return snapPrice(scale.high-ratio*(scale.high-scale.low),step);
}
function zoomScale(scale,factor,anchor,minimum=1e-8,maximum=1e12) {
  if(!scale || !(scale.high>scale.low) || !positive(factor))return scale;
  const oldSpan=scale.high-scale.low,point=Number.isFinite(anchor)?Math.max(scale.low,Math.min(scale.high,anchor)):(scale.high+scale.low)/2;
  const span=Math.max(minimum,Math.min(maximum,oldSpan*factor)),ratio=(point-scale.low)/oldSpan;
  const low=Math.max(0,point-span*ratio);return {low,high:low+span};
}
function createProtectionChart({document,onEdit,onHistoryRetry=()=>{},frames=document.defaultView}) {
  const element=document.createElement('div');element.id='mt5-protection-chart';element.hidden=true;
  const style=document.createElement('style');style.textContent=`
    #mt5-protection-chart{min-width:0;margin:6px 0}
    #mt5-protection-chart .chart-tools{display:flex;align-items:center;gap:5px;min-width:0;flex-wrap:nowrap}
    #mt5-protection-chart .chart-tools button{flex:none}
    #mt5-protection-chart .chart-tools button{padding:2px 8px!important;font-size:11px}
    #mt5-protection-chart .chart-tools button[data-field=sl]{color:#f87171}
    #mt5-protection-chart .chart-tools button[data-field=tp]{color:#4ade80}
    #mt5-protection-chart .chart-tools button[data-field=partialTp]{color:#fbbf24}
    #mt5-protection-chart .chart-tools button[aria-pressed=true]{border-color:currentColor}
    #mt5-protection-chart .chart-caption{margin-left:auto;min-width:0;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-size:10px;color:var(--muted);text-align:right}
    #mt5-protection-chart svg{display:block;width:100%;height:138px;min-width:0;touch-action:none;background:var(--bg,#111);border-radius:8px;cursor:grab}
    #mt5-protection-chart svg[data-panning=true]{cursor:grabbing}
    #mt5-protection-chart svg text{font:11px system-ui,sans-serif;font-variant-numeric:tabular-nums}
    #mt5-protection-chart svg [data-protection-line]{cursor:ns-resize;outline:none}
    #mt5-protection-chart svg [role=slider]:focus rect{stroke:currentColor;stroke-width:1}
    #mt5-protection-chart svg[aria-disabled=true] [data-protection-line]{cursor:default}
    #mt5-protection-chart .chart-hint{font-size:10px;line-height:13px;height:26px;overflow:auto;color:var(--muted);margin-top:3px}
    #mt5-protection-chart .chart-history{font-size:10px;line-height:13px;height:26px;overflow:auto;color:var(--muted);display:flex;align-items:center;gap:6px;margin-top:3px}
    #mt5-protection-chart .chart-history[hidden]{display:flex!important;visibility:hidden}
    #mt5-protection-chart .chart-history button{font-size:10px;padding:2px 6px!important}
  `;
  const tools=document.createElement('div');tools.className='chart-tools';
  const caption=document.createElement('span');caption.className='chart-caption';
  caption.title='Последние 30 реальных минутных баров Bid основного счёта MT5.';
  const choices={},zooms={sl:1,tp:1,partialTp:1},framing={sl:'auto',tp:'level',partialTp:'level'},viewports={sl:null,tp:null,partialTp:null},autoScales={sl:null,tp:null,partialTp:null},viewButtons=[];
  let selected='sl',model=null,identity='',drag=null,pan=null,scale=null;
  let bars=[],barSource='',lastQuoteAt=0,lastQuote=null,lastPaintAt=0,quoteTimer=null,destroyed=false;
  let barRevision=0,lastPaintKey='',barLayer=null,barClip=null,dragFrame=null;
  const requestFrame=frames?.requestAnimationFrame?.bind(frames) || (callback=>setTimeout(callback,16));
  const cancelFrame=frames?.cancelAnimationFrame?.bind(frames) || clearTimeout;
  function cancelDragFrame(){if(dragFrame!==null){cancelFrame(dragFrame);dragFrame=null;}}
  function scheduleDragPaint(){
    if(dragFrame===null && !destroyed)dragFrame=requestFrame(()=>{dragFrame=null;paint();});
  }
  const barNodes=new Map();
  const barTime=new Intl.DateTimeFormat('ru-RU',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit'});
  for(const [key,label] of [['sl','SL'],['tp','TP'],['partialTp','TP%']]){
    const button=document.createElement('button');button.type='button';button.dataset.field=key;button.textContent=label;
    button.title=key==='partialTp'?'Частичный тейк всего набора. Перетащи линию, затем отдельно сохрани частичный тейк.':`Выбери ${label}, затем нажми на графике или перетащи линию. Для отправки нажми «Сохранить».`;
    button.onclick=()=>{drag=pan=null;selected=key;paint();};choices[key]=button;tools.append(button);
  }
  for(const [action,label,title] of [['in','+','Приблизить шкалу'],['out','−','Расширить шкалу'],['auto','Авто','SL: масштаб по минутным барам; TP: запас для дальнего тейка']]){
    const button=document.createElement('button');button.type='button';button.dataset.zoom=action;button.textContent=label;button.title=title;
    button.setAttribute('aria-label',title);
    button.onclick=()=>{drag=pan=null;if(action==='auto')resetView();else changeZoom(action==='out'?1.25:.8);};
    viewButtons.push(button);tools.append(button);
  }
  tools.append(caption);
  const svg=document.createElementNS(NS,'svg');svg.setAttribute('viewBox',`0 0 ${WIDTH} ${HEIGHT}`);
  svg.setAttribute('preserveAspectRatio','none');svg.setAttribute('aria-label','График позиции: вход, текущая цена, стоп и тейк');
  const hint=document.createElement('div');hint.className='chart-hint';hint.textContent='Перетащи SL/TP · затем «Сохранить»';
  hint.title='Перетаскивание фона сдвигает шкалу, колесо плавно приближает цену под мышью. Двойной щелчок возвращает «Авто». Цена меняется только при перетаскивании линии SL/TP; снятый уровень можно задать нажатием на графике.';
  const history=document.createElement('div');history.className='chart-history';history.setAttribute('role','status');
  const historyText=document.createElement('span'),historyRetry=document.createElement('button');
  historyRetry.type='button';historyRetry.textContent='Повторить';historyRetry.title='Загрузить реальные минутные бары';historyRetry.onclick=onHistoryRetry;
  history.append(historyText,historyRetry);
  element.append(style,tools,svg,history,hint);
  function resetView(){viewports[selected]=autoScales[selected]=null;zooms[selected]=1;framing[selected]='auto';paint();}
  function changeZoom(factor,anchor){
    const base=protectionScale({...model,bars},selected,1,framing[selected]);if(!base || !scale)return;
    viewports[selected]=zoomScale(scale,factor,anchor,Math.max(Number(model.tickSize)*2,(base.high-base.low)/64),(base.high-base.low)*1024);paint();
  }
  function node(tag,attributes={},text){
    const result=document.createElementNS(NS,tag);
    for(const [key,value] of Object.entries(attributes))result.setAttribute(key,String(value));
    if(text!==undefined)result.textContent=text;
    return result;
  }
  const text=(element,value)=>{if(element.textContent!==value)element.textContent=value;};
  const property=(element,key,value)=>{if(element[key]!==value)element[key]=value;};
  const attribute=(element,key,value)=>{value=String(value);if(element.getAttribute(key)!==value)element.setAttribute(key,value);};
  // Keep the actual pointer/focus targets alive across quotes and drag frames.
  // Replacing the SVG tree made hover labels and focused handles flicker.
  function syncChildren(parent,nextChildren){
    const key=n=>n.getAttribute('data-protection-line') || n.getAttribute('data-price-line')
      || n.getAttribute('data-minute-bar') || n.getAttribute('id') || n.getAttribute('data-minute-bars');
    const old=[...parent.children],used=new Set();
    nextChildren.forEach((next,index)=>{
      const id=key(next);
      let current=id ? old.find(n=>!used.has(n) && n.tagName===next.tagName && key(n)===id)
        : old.find(n=>!used.has(n) && !key(n) && n.tagName===next.tagName);
      if(!current)current=next;
      else if(current!==next){
        for(const attr of [...current.attributes])if(!next.hasAttribute(attr.name))current.removeAttribute(attr.name);
        for(const attr of [...next.attributes])if(current.getAttribute(attr.name)!==attr.value)current.setAttribute(attr.name,attr.value);
        if(next.children.length)syncChildren(current,[...next.children]);
        else if(current.textContent!==next.textContent)current.textContent=next.textContent;
      }
      used.add(current);
      if(parent.children[index]!==current)parent.insertBefore(current,parent.children[index] || null);
    });
    for(const child of old)if(!used.has(child))child.remove();
  }
  function paint(){
    cancelDragFrame();
    clearTimeout(quoteTimer);quoteTimer=null;
    if(destroyed || !model || element.hidden)return;
    const width=Math.max(220,svg.clientWidth || WIDTH),right=width-106;
    const values={sl:Number(model.sl),tp:Number(model.tp),partialTp:Number(model.partialTp)};
    if(drag)values[drag.key]=drag.price;
    if(!drag && !pan && !viewports[selected] && selected==='sl' && bars.length && framing.sl==='auto'
      && autoScales.sl && positive(model.current) && (Number(model.current)<autoScales.sl.low || Number(model.current)>autoScales.sl.high))autoScales.sl=null;
    scale=drag?.scale || viewports[selected] || autoScales[selected] || protectionScale({...model,...values,bars},selected,zooms[selected],framing[selected]);
    if(scale && !drag && !viewports[selected])autoScales[selected]=scale;
    const paintKey=JSON.stringify([width,selected,model.enabled,model.blocked,model.historyState?.loading,
      model.historyState?.error,values.sl,values.tp,values.partialTp,model.partialPercent,model.partialEnabled,model.entry,model.current,model.side,model.tickSize,
      barRevision,scale?.low,scale?.high,!!pan,framing[selected]]);
    if(paintKey===lastPaintKey)return;
    lastPaintKey=paintKey;
    if(lastQuote)lastPaintAt=Date.now();
    attribute(svg,'viewBox',`0 0 ${width} ${HEIGHT}`);
    const focused=svg.contains(document.activeElement)?document.activeElement.closest('[data-protection-line]')?.dataset.protectionLine:null;
    for(const [key,button] of Object.entries(choices)){property(button,'disabled',!model.enabled);property(button,'hidden',key==='partialTp' && !model.partialEnabled);attribute(button,'aria-pressed',selected===key);}
    for(const button of viewButtons)property(button,'disabled',!model.enabled);
    attribute(svg,'aria-disabled',!model.enabled);
    if(svg.dataset.panning!==String(!!pan))svg.dataset.panning=String(!!pan);
    text(hint,model.enabled?'Перетащи SL/TP · затем «Сохранить»':model.blocked || 'Для графика нужен шаг цены MT5');
    property(history,'hidden',!!bars.length && !model.historyState?.loading && !model.historyState?.error);
    text(historyText,model.historyState?.error?'M1: '+model.historyState.error:model.historyState?.loading?'Загружаю минутные бары…':bars.length?'':'Минутные бары ещё не загружены');
    property(historyRetry,'hidden',!model.historyState?.error);property(historyRetry,'disabled',!!model.historyState?.loading);
    if(!scale){svg.replaceChildren();return;}
    const span=scale.high-scale.low;
    text(caption,(span<10?Number(span.toFixed(1)):Math.round(span)).toLocaleString('ru-RU')+' п.'+(bars.length?` · M1 · ${bars.length}`:''));
    property(caption,'title',`Размах шкалы: ${Number(span.toFixed(2))} пункта. ${selected==='partialTp'?'TP%: масштаб частичного тейка.':selected==='tp'?'TP: запас для дальнего тейка.':framing.sl==='auto' && bars.length?'SL: масштаб по минутным барам.':'SL: масштаб выбранного стопа.'} ${bars.length?'Реальные бары Bid основного счёта MT5; время — Нью-Йорк.':'История M1 загружается; линия содержит только наблюдавшиеся цены.'}`);
    const y=price=>TOP+(scale.high-Number(price))/(scale.high-scale.low)*(BOTTOM-TOP);
    const children=[];
    for(const ratio of [.25,.5,.75])children.push(node('line',{x1:8,x2:width-8,y1:TOP+ratio*(BOTTOM-TOP),y2:TOP+ratio*(BOTTOM-TOP),stroke:'#27272a','stroke-width':.6}));
    if(bars.length){
      if(!barLayer){
        const id='mt5-minute-bar-clip';
        barLayer=node('g',{'data-minute-bars':'M1','clip-path':`url(#${id})`});
        barClip=node('clipPath',{id});barClip.append(node('rect',{x:8,y:TOP,height:BOTTOM-TOP}));
      }
      const clipWidth=String(Math.max(1,right-16)),clipRect=barClip.firstElementChild;
      if(clipRect.getAttribute('width')!==clipWidth)clipRect.setAttribute('width',clipWidth);
      children.push(barClip);
      // Thirty real bars plus two empty time slots before the price labels.
      const slot=(right-16)/32;
      const geometryKey=`${width}:${scale.low}:${scale.high}:${bars.length}`;
      const barChildren=[];
      for(const [index,row] of bars.entries()) {
        const x=8+slot*(30-bars.length+index+.5),tick=Math.min(4,slot*.3);
        let cached=barNodes.get(row[0]);
        if(!cached){
          const group=node('g',{'data-minute-bar':row[0]}),path=node('path',{fill:'none','stroke-width':1.2,'vector-effect':'non-scaling-stroke'}),title=node('title');
          group.append(path,title);cached={group,path,title,time:barTime.format(new Date(row[0]*1000))};barNodes.set(row[0],cached);
        }
        const dataKey=row.join('|'),shapeKey=`${geometryKey}:${index}`;
        if(cached.dataKey!==dataKey || cached.shapeKey!==shapeKey){
          cached.path.setAttribute('d',`M ${x} ${y(row[2])} V ${y(row[3])} M ${x-tick} ${y(row[1])} H ${x} M ${x} ${y(row[4])} H ${x+tick}`);
          cached.path.setAttribute('stroke',index===bars.length-1?'#e4e4e7':'#a1a1aa');
          if(cached.dataKey!==dataKey)cached.title.textContent=`${cached.time} · O ${row[1]} H ${row[2]} L ${row[3]} C ${row[4]}`;
          cached.dataKey=dataKey;cached.shapeKey=shapeKey;
        }
        barChildren.push(cached.group);
      }
      syncChildren(barLayer,barChildren);children.push(barLayer);
    }
    const levels=[['entry',model.entry,'Вход','#8b8b94'],['current',model.current,'Цена','#d4d4d8'],['sl',values.sl,'SL','#f87171'],['tp',values.tp,'TP','#4ade80'],['partialTp',values.partialTp,`TP${model.partialPercent || 50}%`,'#fbbf24']].filter(([,value])=>positive(value));
    const labels=new Map(),ordered=[...levels].sort((a,b)=>y(a[1])-y(b[1]));
    let previous=-8;
    for(const [key,value] of ordered){const labelY=Math.max(10,y(value),previous+18);labels.set(key,labelY);previous=labelY;}
    let next=HEIGHT+8;
    for(const [key] of ordered.reverse()){const labelY=Math.min(labels.get(key),HEIGHT-10,next-18);labels.set(key,labelY);next=labelY;}
    for(const [key,value,label,color] of levels){
      const outside=Number(value)<scale.low || Number(value)>scale.high;
      const lineY=Math.max(TOP,Math.min(BOTTOM,y(value))),editable=key==='sl'||key==='tp'||key==='partialTp' && model.partialEnabled;
      const group=node('g',editable?{'data-protection-line':key,'data-offscale':String(outside),role:outside?'button':'slider',tabindex:model.enabled?'0':'-1','aria-label':`${label} · ${value}${outside?' · вне шкалы; нажми для своего масштаба':''}`,'aria-valuenow':value,'aria-valuetext':String(value),'aria-orientation':'vertical',style:`color:${color}`} : {'data-price-line':key});
      group.append(node('line',{x1:8,x2:right,y1:lineY,y2:lineY,stroke:color,'stroke-width':editable?1.3:.8,'stroke-dasharray':editable && !outside?'':'3 4','vector-effect':'non-scaling-stroke'}));
      if(editable)group.append(node('line',{x1:8,x2:width-8,y1:lineY,y2:lineY,stroke:'transparent','stroke-width':18}));
      const labelY=Math.max(10,Math.min(HEIGHT-10,labels.get(key)));
      if(Math.abs(labelY-lineY)>1)group.append(node('line',{x1:right,x2:right+3,y1:lineY,y2:labelY,stroke:color,'stroke-width':.5}));
      group.append(node('rect',{x:right+2,y:labelY-9,width:100,height:18,rx:4,fill:'var(--bg,#111)'}));
      group.append(node('text',{x:right+7,y:labelY+4,fill:color},`${outside?(Number(value)>scale.high?'↑ ':'↓ '):''}${label} ${Number(Number(value).toFixed(8))}`));
      children.push(group);
    }
    syncChildren(svg,children);
    if(focused && model.enabled && !svg.contains(document.activeElement))svg.querySelector(`[data-protection-line=${focused}]`)?.focus({preventScroll:true});
  }
  function pointerValue(event){
    const box=svg.getBoundingClientRect();
    return box.height>0?priceAt((event.clientY-box.top)*HEIGHT/box.height-(drag?.offset || 0),drag?.scale || scale,model?.tickSize):null;
  }
  function panTo(event){
    const box=svg.getBoundingClientRect();if(!(box.height>0))return false;
    const offset=(event.clientY-pan.y)*HEIGHT/box.height/(BOTTOM-TOP)*(pan.scale.high-pan.scale.low);
    const low=Math.max(0,pan.scale.low+offset);viewports[selected]={low,high:low+pan.scale.high-pan.scale.low};return true;
  }
  svg.addEventListener('pointerdown',event=>{
    if(event.button!==0 || !model?.enabled || !scale)return;
    const hit=event.target.closest('[data-protection-line]'),key=hit?.dataset.protectionLine || selected;
    const outside=hit?.dataset.offscale==='true';
    if(outside){selected=key;zooms[key]=1;framing[key]='level';viewports[key]=autoScales[key]=null;paint();event.preventDefault();return;}
    if(!hit && positive(model[key])){
      pan={pointerId:event.pointerId,y:event.clientY,scale:{...scale},identity};
      try{svg.setPointerCapture(event.pointerId);}catch{}
      event.preventDefault();paint();return;
    }
    if(hit && key!==selected){
      viewports[key]={...scale};selected=key;paint();
    }
    const value=hit?snapPrice(model[key],model.tickSize):pointerValue(event);if(value===null)return;
    const box=svg.getBoundingClientRect(),lineY=TOP+(scale.high-value)/(scale.high-scale.low)*(BOTTOM-TOP);
    const offset=hit && box.height>0?(event.clientY-box.top)*HEIGHT/box.height-lineY:0;
    selected=key;drag={key,price:value,scale,identity,pointerId:event.pointerId,offset};
    try{svg.setPointerCapture(event.pointerId);}catch{ /* Synthetic or cancelled pointer. */ }
    event.preventDefault();paint();
  });
  svg.addEventListener('pointermove',event=>{
    if(pan && pan.pointerId===event.pointerId && model?.enabled){
      if(!panTo(event))return;
      event.preventDefault();scheduleDragPaint();return;
    }
    if(!drag || drag.pointerId!==event.pointerId || !model?.enabled)return;
    const value=pointerValue(event);if(value===null)return;
    event.preventDefault();if(drag.price===value)return;
    drag.price=value;scheduleDragPaint();
  });
  svg.addEventListener('pointerup',event=>{
    if(pan?.pointerId===event.pointerId){if(model?.enabled)panTo(event);pan=null;paint();return;}
    if(!drag || drag.pointerId!==event.pointerId)return;
    // The release can arrive before the queued frame or without a last move.
    const value=pointerValue(event);if(value!==null)drag.price=value;
    const final=drag;drag=null;cancelDragFrame();
    if(final.identity===identity && model?.enabled){viewports[final.key]={...final.scale};onEdit(final.key,String(final.price));}
    paint();
  });
  const cancel=()=>{drag=pan=null;paint();};
  svg.addEventListener('pointercancel',cancel);svg.addEventListener('lostpointercapture',cancel);
  svg.addEventListener('wheel',event=>{
    if(!model?.enabled || !scale)return;
    event.preventDefault();drag=pan=null;
    const box=svg.getBoundingClientRect(),ratio=box.height>0?Math.max(0,Math.min(1,((event.clientY-box.top)*HEIGHT/box.height-TOP)/(BOTTOM-TOP))):.5;
    changeZoom(Math.exp(Math.max(-2,Math.min(2,event.deltaY*.002))),scale.high-ratio*(scale.high-scale.low));
  },{passive:false});
  svg.addEventListener('dblclick',event=>{if(model?.enabled){event.preventDefault();drag=pan=null;resetView();}});
  svg.addEventListener('keydown',event=>{
    if(event.key==='Escape' && (drag || pan)){cancel();event.preventDefault();event.stopPropagation();return;}
    const key=event.target.closest('[data-protection-line]')?.dataset.protectionLine;
    if(key && model?.enabled && ['Enter',' '].includes(event.key)){
      event.preventDefault();drag=null;selected=key;
      if(event.target.closest('[data-protection-line]').dataset.offscale==='true'){zooms[key]=1;framing[key]='level';viewports[key]=autoScales[key]=null;}
      paint();return;
    }
    if(!key || !model?.enabled || !['ArrowUp','ArrowDown'].includes(event.key))return;
    const value=snapPrice(Number(model[key])+(event.key==='ArrowUp'?1:-1)*(event.shiftKey?10:1)*model.tickSize,model.tickSize);
    if(value!==null){event.preventDefault();selected=key;onEdit(key,String(value));paint();svg.querySelector(`[data-protection-line=${key}]`)?.focus();}
  });
  const observer=document.defaultView?.ResizeObserver?new document.defaultView.ResizeObserver(()=>{if(element.getClientRects().length)paint();}):null;
  observer?.observe(svg);
  return {element,update(next){
    if(next.identity!==identity){
      clearTimeout(quoteTimer);quoteTimer=null;lastPaintKey='';barNodes.clear();barLayer?.replaceChildren();
      identity=next.identity;bars=[];barSource='';lastQuoteAt=lastPaintAt=0;lastQuote=null;drag=pan=null;selected='sl';zooms.sl=zooms.tp=zooms.partialTp=1;viewports.sl=viewports.tp=viewports.partialTp=autoScales.sl=autoScales.tp=autoScales.partialTp=null;framing.sl='auto';framing.tp=framing.partialTp='level';
    }
    const history=next.minuteHistory;
    if(history && history.capturedAt!==barSource && Array.isArray(history.bars)) {
      if(!bars.length && history.bars.length && !drag && !pan)autoScales.sl=null;
      bars=history.bars.slice(-30).map(row=>row.slice());barSource=history.capturedAt;
      const last=bars.at(-1);
      // A slow history reply cannot roll a newer tick back. Never extend the
      // preceding minute with a tick from a minute not supplied by MT5 yet.
      if(lastQuote && last && Math.floor(lastQuote.at/60000)*60===last[0]
        && (!Number.isFinite(history.capturedAt) || lastQuote.at>history.capturedAt)){
        last[2]=Math.max(last[2],lastQuote.bid);last[3]=Math.min(last[3],lastQuote.bid);last[4]=lastQuote.bid;
      }
      const present=new Set(bars.map(row=>row[0]));for(const key of barNodes.keys())if(!present.has(key))barNodes.delete(key);
      barRevision++;
    }
    model={...next,enabled:next.enabled===true && positive(next.tickSize)};
    if(selected==='partialTp' && !model.partialEnabled){selected='sl';drag=pan=null;}
    // The Bid bar and its price marker always use the same accepted snapshot.
    // General position refreshes must not overwrite this with a slower price.
    if(bars.length)model.current=bars.at(-1)[4];
    else if(lastQuote)model.current=lastQuote.bid;
    if(!model.enabled)drag=pan=null;
    element.hidden=!next.position;
    paint();
  },quote({bid,at}){
    if(destroyed || element.hidden || !positive(bid) || !Number.isFinite(at) || at<=lastQuoteAt)return;
    lastQuoteAt=at;lastQuote={bid:Number(bid),at};
    const last=bars.at(-1);
    if(last && Math.floor(at/60000)*60!==last[0])return;
    if(model.current===Number(bid) && (!last || last[4]===Number(bid)))return;
    model.current=Number(bid);
    if(last){last[2]=Math.max(last[2],bid);last[3]=Math.min(last[3],bid);last[4]=Number(bid);barRevision++;}
    const remaining=250-(Date.now()-lastPaintAt);
    if(remaining<=0)paint();
    else if(quoteTimer===null){quoteTimer=setTimeout(()=>{quoteTimer=null;paint();},remaining);quoteTimer.unref?.();}
  },cancel,destroy(){destroyed=true;cancelDragFrame();clearTimeout(quoteTimer);quoteTimer=null;observer?.disconnect();barNodes.clear();drag=pan=null;}};
}
module.exports={createProtectionChart,snapPrice,chartScale,protectionScale,priceAt,zoomScale};
