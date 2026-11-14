import{p as et}from"./chunk-6ZKBGPIT-DZEK8i2m.js";import{p as at}from"./cynefin-OW5HDTMX-4GGFXZVS-Cv3IESDD.js";import{g as rt,s as it,a as nt,b as ot,q as st,p as lt,_ as l,l as z,c as ct,E as dt,a3 as pt,a4 as gt,a5 as U,a6 as ht,d as ut,t as ft,a7 as mt,F as vt}from"./md-DSy7XMU5.js";import"./index-yS1LDbr1.js";import"./modules/vue-Bk_yUYT3.js";import"./modules/shiki-BRXnlo-C.js";import"./modules/file-saver-B8IIMB9x.js";import"./default-2a2pH9nl.js";import"./CornerCurves.vue_vue_type_script_setup_true_lang-uXk8QUAY.js";import"./slidev/context-AKhieYwT.js";var St=vt.pie,R={sections:new Map,showData:!1},T=R.sections,F=R.showData,xt=structuredClone(St),wt=l(()=>structuredClone(xt),"getConfig"),Ct=l(()=>{T=new Map,F=R.showData,ft()},"clear"),$t=l(({label:t,value:a})=>{if(a<0)throw new Error(`"${t}" has invalid value: ${a}. Negative values are not allowed in pie charts. All slice values must be >= 0.`);T.has(t)||(T.set(t,a),z.debug(`added new section: ${t}, with value: ${a}`))},"addSection"),Dt=l(()=>T,"getSections"),yt=l(t=>{F=t},"setShowData"),Tt=l(()=>F,"getShowData"),q={getConfig:wt,clear:Ct,setDiagramTitle:lt,getDiagramTitle:st,setAccTitle:ot,getAccTitle:nt,setAccDescription:it,getAccDescription:rt,addSection:$t,getSections:Dt,setShowData:yt,getShowData:Tt},bt=l((t,a)=>{et(t,a),a.setShowData(t.showData),t.sections.map(a.addSection)},"populateDb"),At={parse:l(async t=>{const a=await at("pie",t);z.debug(a),bt(a,q)},"parse")},_t=l(t=>`
  .pieCircle{
    stroke: ${t.pieStrokeColor};
    stroke-width : ${t.pieStrokeWidth};
    opacity : ${t.pieOpacity};
  }
  .pieCircle.highlighted{
    scale: 1.05;
    opacity: 1;
  }
  .pieCircle.highlightedOnHover:hover{
    transition-duration: 250ms;
    scale: 1.05;
    opacity: 1;
  }
  .pieOuterCircle{
    stroke: ${t.pieOuterStrokeColor};
    stroke-width: ${t.pieOuterStrokeWidth};
    fill: none;
  }
  .pieTitleText {
    text-anchor: middle;
    font-size: ${t.pieTitleTextSize};
    fill: ${t.pieTitleTextColor};
    font-family: ${t.fontFamily};
  }
  .slice {
    font-family: ${t.fontFamily};
    fill: ${t.pieSectionTextColor};
    font-size:${t.pieSectionTextSize};
    // fill: white;
  }
  .legend text {
    fill: ${t.pieLegendTextColor};
    font-family: ${t.fontFamily};
    font-size: ${t.pieLegendTextSize};
  }
`,"getStyles"),kt=_t,Et=l(t=>{const a=[...t.values()].reduce((o,m)=>o+m,0),L=[...t.entries()].map(([o,m])=>({label:o,value:m})).filter(o=>o.value/a*100>=1);return mt().value(o=>o.value).sort(null)(L)},"createPieArcs"),zt=l((t,a,L,W)=>{var I;z.debug(`rendering pie chart
`+t);const o=W.db,m=ct(),h=dt(o.getConfig(),m.pie),H=40,i=18,c=4,C=450,S=C,b=pt(a),$=b.append("g");$.attr("transform","translate("+S/2+","+C/2+")");const{themeVariables:n}=m;let[M]=gt(n.pieOuterStrokeWidth);M??(M=2);const V=h.legendPosition,O=h.textPosition,X=h.donutHole>0&&h.donutHole<=.9?h.donutHole:0,u=Math.min(S,C)/2-H,Z=U().innerRadius(X*u).outerRadius(u),j=U().innerRadius(u*O).outerRadius(u*O),x=$.append("g");x.append("circle").attr("cx",0).attr("cy",0).attr("r",u+M/2).attr("class","pieOuterCircle");const D=o.getSections(),J=Et(D),K=[n.pie1,n.pie2,n.pie3,n.pie4,n.pie5,n.pie6,n.pie7,n.pie8,n.pie9,n.pie10,n.pie11,n.pie12];let A=0;D.forEach(e=>{A+=e});const P=J.filter(e=>(e.data.value/A*100).toFixed(0)!=="0"),_=ht(K).domain([...D.keys()]);x.selectAll("mySlices").data(P).enter().append("path").attr("d",Z).attr("fill",e=>_(e.data.label)).attr("class",e=>{let r="pieCircle";return h.highlightSlice==="hover"?r+=" highlightedOnHover":h.highlightSlice===e.data.label&&(r+=" highlighted"),r}),x.selectAll("mySlices").data(P).enter().append("text").text(e=>(e.data.value/A*100).toFixed(0)+"%").attr("transform",e=>"translate("+j.centroid(e)+")").style("text-anchor","middle").attr("class","slice");const Q=$.append("text").text(o.getDiagramTitle()).attr("x",0).attr("y",-400/2).attr("class","pieTitleText"),w=[...D.entries()].map(([e,r])=>({label:e,value:r})),f=$.selectAll(".legend").data(w).enter().append("g").attr("class","legend");f.append("rect").attr("width",i).attr("height",i).style("fill",e=>_(e.label)).style("stroke",e=>_(e.label)),f.append("text").attr("x",i+c).attr("y",i-c).text(e=>o.getShowData()?`${e.label} [${e.value}]`:e.label);const v=Math.max(...f.selectAll("text").nodes().map(e=>(e==null?void 0:e.getBoundingClientRect().width)??0));let y=C,k=S+H;const s=i+c,E=w.length*s;switch(V){case"center":f.attr("transform",(e,r)=>{const d=s*w.length/2,p=-v/2-(i+c),g=r*s-d;return"translate("+p+","+g+")"});break;case"top":y+=E,f.attr("transform",(e,r)=>{const d=u,p=-v/2-(i+c),g=r*s-d;return`translate(${p}, ${g})`}),x.attr("transform",()=>`translate(0, ${E+s})`);break;case"bottom":y+=E,f.attr("transform",(e,r)=>{const d=-u-s,p=-v/2-(i+c),g=r*s-d;return"translate("+p+","+g+")"});break;case"left":k+=i+c+v,f.attr("transform",(e,r)=>{const d=s*w.length/2,p=-u-(i+c),g=r*s-d;return"translate("+p+","+g+")"}),x.attr("transform",()=>`translate(${v+i+c}, 0)`);break;case"right":default:k+=i+c+v,f.attr("transform",(e,r)=>{const d=s*w.length/2,p=12*i,g=r*s-d;return"translate("+p+","+g+")"});break}const G=((I=Q.node())==null?void 0:I.getBoundingClientRect().width)??0,Y=S/2-G/2,tt=S/2+G/2,B=Math.min(0,Y),N=Math.max(k,tt)-B;b.attr("viewBox",`${B} 0 ${N} ${y}`),ut(b,y,N,h.useMaxWidth)},"draw"),Rt={draw:zt},Ut={parser:At,db:q,renderer:Rt,styles:kt};export{Ut as diagram};
