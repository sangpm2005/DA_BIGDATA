"use strict";
const $ = (id) => document.getElementById(id);
const colors = {Shopee:"#ed7657",Lazada:"#7a63d7",Sendo:"#c9963f",Tiki:"#2b9cae"};
const tints = {Shopee:"#fff1eb",Lazada:"#f2effc",Sendo:"#fbf4e5",Tiki:"#eaf7fa"};
const names = {overview:"Tổng quan",aspects:"Khía cạnh",reviews:"Tra cứu phản hồi",pipeline:"Luồng dữ liệu"};
const headings = {overview:["Từ phản hồi đến ưu tiên cải thiện.","Nhìn thấy xu hướng, tìm khía cạnh và đối chiếu bằng phản hồi thực tế."],aspects:["Tìm những chủ đề cần được lắng nghe.","So sánh đề cập khía cạnh, kiểm tra độ bao phủ và mở phản hồi minh họa."],reviews:["Đối chiếu bằng tiếng nói của người dùng.","Tra cứu trên toàn bộ review đã làm sạch, không chỉ một tập ví dụ chọn trước."],pipeline:["Theo dõi hành trình của dữ liệu.","Giải thích xử lý phân tán, điều phối và phục hồi bằng kết quả chạy thật."]};
const advice = {app:"Rà soát phản hồi về đăng nhập, cập nhật và lỗi thao tác ứng dụng.",delivery:"Đối chiếu thời gian giao, phí vận chuyển và tình huống giao sai hoặc thiếu.",product:"Kiểm tra phản hồi về chất lượng, khác mô tả và đóng gói.",support:"Xem tình huống tư vấn, tiếp nhận và xử lý khiếu nại.",payment:"Đối chiếu lỗi thanh toán, trừ tiền và phương thức thanh toán.",promotion:"Kiểm tra điều kiện mã giảm giá, mức giá và khuyến mãi.",returns:"Rà soát trải nghiệm hủy đơn, đổi trả và hoàn tiền."};
const state = {boot:null,data:null,view:"overview",filters:null,trend:"rate",offset:0,reviewResult:null,reviewToken:0};
const num = (n) => n == null ? "—" : Number(n).toLocaleString("vi-VN");
const pct = (n) => n == null ? "—" : (Number(n)*100).toLocaleString("vi-VN",{minimumFractionDigits:2,maximumFractionDigits:2})+"%";
const dec = (n,d=2) => n == null ? "—" : Number(n).toLocaleString("vi-VN",{minimumFractionDigits:d,maximumFractionDigits:d});
const esc = (value) => String(value ?? "").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const label = (code) => state.boot.metadata.aspects.find(a=>a.code===code)?.label || code;
const symbol = (p) => `<span class="platform-symbol" style="--color:${colors[p]};--tint:${tints[p]}">${p[0]}</span>`;

async function api(path){const response=await fetch(path);let result;try{result=await response.json();}catch{throw new Error("Không đọc được phản hồi từ máy chủ.");}if(!response.ok)throw new Error(result.error||"Không đọc được dữ liệu.");return result;}
function params(extra={}){return new URLSearchParams({...state.filters,platforms:state.filters.platforms.join(","),exclude_spam:state.filters.exclude_spam?"1":"0",...extra});}
function toast(message){$("toast").textContent=message;$("toast").classList.remove("hidden");clearTimeout(state.toastTimer);state.toastTimer=setTimeout(()=>$("toast").classList.add("hidden"),5000);}
function showError(error){$("error").textContent=error.message;$("error").classList.remove("hidden");}
function tip(text,event){const t=$("tooltip");t.textContent=text;t.classList.remove("hidden");t.style.left=Math.min(event.clientX+13,window.innerWidth-275)+"px";t.style.top=Math.max(8,Math.min(event.clientY-60,window.innerHeight-100))+"px";}
function wireTips(root){root.querySelectorAll("[data-tip]").forEach(node=>{node.addEventListener("pointermove",event=>tip(node.getAttribute("data-tip"),event));node.addEventListener("pointerleave",()=>$("tooltip").classList.add("hidden"));});}
function svg(w,h,inside,title){return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(title)}" font-family="Segoe UI, Arial, sans-serif">${inside}</svg>`;}
function text(x,y,value,extra=""){return `<text x="${x}" y="${y}" ${extra.includes('fill=')?'':'fill="#95a6af"'} ${extra.includes('font-size=')?'':'font-size="10"'} ${extra}>${esc(value)}</text>`;}
function monthRange(from,to){const result=[];let [year,month]=from.split("-").map(Number);while(`${year}-${String(month).padStart(2,"0")}`<=to){result.push(`${year}-${String(month).padStart(2,"0")}`);if(++month>12){month=1;year++;}}return result;}

function renderOverview(){
 const d=state.data,s=d.stats;
 $("kpis").innerHTML=[
  ["Phản hồi trong phạm vi đang chọn",num(s.reviews),"review từ Parquet HDFS","#2a7c72"],
  ["Đánh giá thấp · 1–2 sao",num(s.low),`<b>${pct(s.rate)}</b> trên ${num(s.reviews)} review`,"#d97559"],
  ["Điểm đánh giá trung bình",`${dec(s.mean)} <small>/ 5</small>`,"Tính theo số review của từng mức sao","#8070c3"],
  ["Review sao thấp có đề cập khía cạnh",pct(s.coverage),`${num(s.low-s.unmatched)} / ${num(s.low)} review sao thấp`,"#458f9a"]
 ].map(([title,value,detail,color])=>`<div class="kpi" style="--accent:${color}"><div class="kpi-label">${title}</div><div class="kpi-value">${value}</div><div class="kpi-detail">${detail}</div></div>`).join("");
 const max=Math.max(.3,...d.platforms.map(p=>p.rate||0));
 $("platform-comparison").innerHTML=d.platforms.map(p=>`<div class="comparison-row"><div class="comparison-title"><span>${symbol(p.platform)} ${p.platform}</span><strong>${pct(p.rate)}</strong></div><div class="bar-track"><div class="bar-fill" style="background:${colors[p.platform]};width:${(p.rate||0)/max*100}%"></div></div><div class="comparison-bottom"><span>${num(p.low)} review sao thấp</span><span>/ ${num(p.reviews)} review</span></div></div>`).join("");
 const top=[...d.aspects].sort((a,b)=>b.mentions-a.mentions).slice(0,3);
 $("priority-list").innerHTML=top.map((a,i)=>`<div class="priority-row"><span class="priority-rank">${i+1}</span><div class="priority-text"><b>${esc(a.label)}</b><small>${esc(advice[a.code])}</small></div><div class="priority-metric">${num(a.mentions)}<small>${pct(a.denominator?a.mentions/a.denominator:null)} sao thấp</small></div><button class="text-button drill-aspect" data-aspect="${a.code}" aria-label="Tra cứu ${esc(a.label)}">↗</button></div>`).join("");
 document.querySelectorAll(".drill-aspect").forEach(button=>button.onclick=()=>drill(button.dataset.aspect));
 renderTrend();renderRatings();renderAspects();
}

function renderTrend(){
 const d=state.data;if(!d)return;const yearly=$("granularity").value==="year";
 const buckets=yearly?Array.from({length:Number(state.filters.to.slice(0,4))-Number(state.filters.from.slice(0,4))+1},(_,i)=>String(Number(state.filters.from.slice(0,4))+i)):monthRange(state.filters.from,state.filters.to);
 const groups={};d.series.forEach(p=>{const key=p.platform+":"+(yearly?p.month.slice(0,4):p.month);const point=groups[key]||(groups[key]={reviews:0,low:0});point.reviews+=p.reviews;point.low+=p.low;});
 const series=d.platforms.map(p=>({platform:p.platform,points:buckets.map(bucket=>{const point=groups[p.platform+":"+bucket];return {bucket,...(point||{reviews:0,low:0}),value:point&&point.reviews?(state.trend==="rate"?point.low/point.reviews:point.reviews):null};})}));
 const w=660,h=257,l=45,r=12,t=16,b=32,pw=w-l-r,ph=h-t-b;
 const values=series.flatMap(s=>s.points.map(p=>p.value).filter(v=>v!==null));
 const largest=Math.max(...values,0);const ymax=state.trend==="rate"?Math.max(.1,Math.ceil(largest*10)/10):Math.max(10,Math.ceil(largest/10000)*10000);
 const x=i=>l+(buckets.length===1?pw/2:i/(buckets.length-1)*pw),y=v=>t+ph-v/ymax*ph;
 let body="";
 for(let i=0;i<5;i++){const v=ymax*i/4;body+=`<line x1="${l}" y1="${y(v)}" x2="${w-r}" y2="${y(v)}" stroke="#edf1f3"/>`+text(l-10,y(v)+3,state.trend==="rate"?Math.round(v*100)+"%":v>=1000?Math.round(v/1000)+"k":Math.round(v),'text-anchor="end"');}
 const step=Math.max(1,Math.ceil(buckets.length/8));buckets.forEach((bucket,i)=>{if(i%step===0||i===buckets.length-1){body+=text(x(i),h-9,yearly?bucket:bucket.slice(5)+"/"+bucket.slice(2,4),'text-anchor="middle"');}});
 series.forEach(s=>{let path="",connected=false;s.points.forEach((p,i)=>{if(p.value===null){connected=false;return;}path+=(connected?" L ":" M ")+x(i)+" "+y(p.value);connected=true;});body+=`<path d="${path}" fill="none" stroke="${colors[s.platform]}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>`;s.points.forEach((p,i)=>{if(p.value===null)return;body+=`<circle cx="${x(i)}" cy="${y(p.value)}" r="${yearly?3.5:2}" fill="${colors[s.platform]}"/><circle cx="${x(i)}" cy="${y(p.value)}" r="8" fill="transparent" data-tip="${esc(s.platform+" · "+p.bucket+"\n"+pct(p.low/p.reviews)+" sao thấp\n"+num(p.low)+" / "+num(p.reviews)+" review")}"/>`;});});
 $("trend-chart").innerHTML=svg(w,h,body,"Xu hướng đánh giá thấp theo thời gian");wireTips($("trend-chart"));
 $("trend-legend").innerHTML=d.platforms.map(p=>`<span style="--color:${colors[p.platform]}">${p.platform}</span>`).join("");
}

function renderRatings(){const values=[0,0,0,0,0];state.data.platforms.forEach(p=>p.ratings.forEach((v,i)=>values[i]+=v));const shades=["#d47762","#e9b298","#d3bd85","#a6cbb7","#5b9e89"],w=470,h=235,l=47,r=82,top=17,gap=42,max=Math.max(...values,1);let body="";for(let i=4;i>=0;i--){const y=top+(4-i)*gap;body+=text(0,y+15,(i+1)+" sao")+`<rect x="${l}" y="${y}" width="${w-l-r}" height="21" rx="4" fill="#f4f6f7"/><rect x="${l}" y="${y}" width="${values[i]/max*(w-l-r)}" height="21" rx="4" fill="${shades[i]}"/>`+text(w-r+10,y+14,num(values[i]))+text(w-r+10,y+27,pct(state.data.stats.reviews?values[i]/state.data.stats.reviews:null),'font-size="8"');}$("rating-chart").innerHTML=svg(w,h,body,"Phân bố năm mức sao");}

function renderAspects(){
 const d=state.data;
 $("aspect-heatmap").innerHTML=`<table class="heatmap"><thead><tr><th>Khía cạnh được đề cập</th>${d.platforms.map(p=>`<th>${symbol(p.platform)} ${p.platform}<br><small>${num(p.low)} review sao thấp</small></th>`).join("")}</tr></thead><tbody>${[...d.aspects].sort((a,b)=>b.mentions-a.mentions).map(a=>`<tr><td class="heat-row-label">${esc(a.label)}</td>${d.platforms.map(p=>{const n=p.aspects[a.code],rate=p.low?n/p.low:null;return `<td><button class="heat-cell" style="background:rgba(64,147,128,${rate===null?.03:.06+Math.min(rate/.3,1)*.38})" data-aspect="${a.code}" data-platform="${p.platform}" title="${esc(a.label+" · "+p.platform+": "+num(n)+" / "+num(p.low)+" review sao thấp")}">${pct(rate)}<small>${num(n)} review</small></button></td>`;}).join("")}</tr>`).join("")}</tbody></table>`;
 $("aspect-heatmap").querySelectorAll("button").forEach(button=>button.onclick=()=>drill(button.dataset.aspect,button.dataset.platform));
 $("coverage-chart").innerHTML=`<div class="legend"><span style="--color:#5a9c84">Có đề cập khía cạnh</span><span style="--color:#dce5e9">Chưa gán</span></div>`+d.platforms.map(p=>`<div class="coverage-row"><div class="coverage-label"><span>${p.platform}</span><b>${pct(p.low?(p.low-p.unmatched)/p.low:null)}</b></div><div class="bar-track"><div class="bar-fill" style="background:#74aa94;width:${p.low?(p.low-p.unmatched)/p.low*100:0}%"></div></div><div class="comparison-bottom"><span>${num(p.low-p.unmatched)} có đề cập</span><span>${num(p.unmatched)} chưa gán</span></div></div>`).join("");
 $("investigation-list").innerHTML=[...d.aspects].sort((a,b)=>b.mentions-a.mentions).slice(0,3).map(a=>`<div class="investigation"><b>${esc(a.label)} · ${num(a.mentions)} review</b><p>${esc(advice[a.code])} Mở phản hồi để phân biệt khen, chê và trường hợp nhắc chung.</p></div>`).join("");
 const c=state.boot.clusters;$("cluster-caption").textContent=`K = ${c.k||"—"} · ${num(c.training_rows)} review trong mẫu hash ${c.sample_percent||25}% đủ điều kiện · TF-IDF + khoảng cách cosine. Phần cụm dùng toàn snapshot, không đổi theo bộ lọc trang; chưa có nhãn người đọc.`;
 $("silhouette-badge").textContent=`Silhouette ${dec(c.silhouette_cosine,3)} · Phân tách yếu`;
 $("cluster-grid").innerHTML=(c.clusters||[]).map(cluster=>{const example=(c.examples||[]).find(e=>e.prediction===cluster.cluster);return `<article class="cluster-card"><h3>Cụm khám phá ${cluster.cluster+1}</h3><div class="terms">${cluster.top_terms.slice(0,12).map(term=>`<span class="term">${esc(term)}</span>`).join("")}</div>${example?`<div class="cluster-quote">“${esc(example.content)}”<br><small>${esc(example.platform)} · ví dụ trong mẫu phân cụm</small></div>`:""}</article>`;}).join("");
}

function renderPipeline(){
 const boot=state.boot,q=boot.quality,e=boot.evidence,v=boot.validation;
 const stages=["Chuẩn bị HDFS","Upload CSV nguồn","Đọc bằng Spark","Làm sạch → Parquet","Spark SQL","Khía cạnh & K-Means","Lưu kết quả","Kafka & Structured Streaming","Đối soát dữ liệu thật"];
 const tasks=e.airflow?.tasks||[];$("dag-badge").textContent=`${tasks.filter(t=>t.state==="success").length}/9 tác vụ đạt · ${e.spark_alive_workers||"—"} worker / ${e.spark_total_cores||"—"} core`;
 $("quality-flow").innerHTML=[["Record gốc",q.raw_rows],["Quarantine",q.quarantined_rows],["Bản sao ID loại bỏ",q.duplicate_valid_id_rows_removed],["Review được giữ",q.clean_rows]].map(([name,value])=>`<div class="quality-box"><span>${name}</span><b>${num(value)}</b></div>`).join("");
 $("dag-steps").innerHTML=stages.map((name,i)=>`<div class="dag-step"><span class="step-number">${String(i+1).padStart(2,"0")}</span><span>${name}</span><span>${tasks[i]?.state==="success"?"✓ Thành công":"Chưa có bằng chứng"}</span></div>`).join("");
 $("stream-stats").innerHTML=`<div><b>${num(e.kafka_total_offsets)}</b><small>Offsets Kafka sau hai lượt replay</small></div><div><b>${num(v.streaming_unique_keys)}</b><small>Review duy nhất trên HDFS</small></div>`;
 const rows=boot.stream_batches,w=480,h=230,l=30,baseline=195,gap=(w-2*l)/Math.max(rows.length,1),max=Math.max(...rows.map(r=>r.new_unique_events),1);let body=`<line x1="${l}" x2="${w-l}" y1="${baseline}" y2="${baseline}" stroke="#dfe7ea"/>`;
 rows.forEach((r,i)=>{const height=r.new_unique_events/max*150,x=l+i*gap+12;body+=`<rect x="${x}" y="${baseline-Math.max(height,2)}" width="${Math.max(gap-24,6)}" height="${Math.max(height,2)}" rx="4" fill="${r.new_unique_events?"#72a994":"#d7e3e7"}"/>`+text(x+(gap-24)/2,baseline-height-9,num(r.new_unique_events),'text-anchor="middle"')+text(x+(gap-24)/2,218,"Batch "+r.batch_id,'text-anchor="middle"');});$("stream-chart").innerHTML=svg(w,h,body,"Review duy nhất mới trong các microbatch");
 const lines=(boot.query_plan||"").split("\n");const start=lines.findIndex(line=>line.includes("Physical Plan"));$("query-plan").textContent=lines.slice(start>=0?start:0).join("\n");
}

function reviewParams(){const p=$("review-platform").value;return params({platforms:p==="all"?state.filters.platforms.join(","):p,score:$("review-score").value,aspect:$("review-aspect").value,q:$("review-query").value,offset:state.offset,limit:20});}
async function loadReviews(){
 const token=++state.reviewToken;$("review-summary").textContent="Đang tìm trong chỉ mục toàn bộ dữ liệu…";$("previous-page").disabled=true;$("next-page").disabled=true;
 try{const result=await api("/api/reviews?"+reviewParams());if(token!==state.reviewToken)return;state.reviewResult=result;
 $("review-summary").textContent=`${num(result.total)} phản hồi phù hợp · ${state.filters.from} → ${state.filters.to}${state.filters.exclude_spam?" · đã loại ứng viên spam":""}`;
 $("review-results").innerHTML=result.rows.length?result.rows.map(r=>`<article class="review"><div class="review-top"><div class="review-meta">${symbol(r.platform)}<b>${esc(r.platform)}</b><span class="stars" aria-label="${r.score} sao">${"★".repeat(r.score)}${"☆".repeat(5-r.score)}</span><span>${esc(r.at.slice(0,10).split("-").reverse().join("/"))}</span>${r.spam?'<span class="review-tag unknown">Ứng viên spam</span>':""}</div><span class="review-key">${esc(r.review_key.slice(0,12))}</span></div><div class="review-content">${esc(r.content)}</div><div class="review-tags">${r.aspects.length?r.aspects.map(code=>`<span class="review-tag">${esc(label(code))}</span>`).join(""):'<span class="review-tag unknown">Chưa gán khía cạnh</span>'}</div></article>`).join(""):'<div class="empty">Không tìm thấy phản hồi phù hợp. Thử đổi từ khóa hoặc bộ lọc.</div>';
 $("page-label").textContent=result.total?`${num(result.offset+1)}–${num(result.offset+result.rows.length)} / ${num(result.total)}`:"0 kết quả";
 $("previous-page").disabled=result.offset===0;$("next-page").disabled=result.offset+result.rows.length>=result.total;
 }catch(error){if(token!==state.reviewToken)return;$("review-summary").textContent=error.message;$("review-results").innerHTML="";toast(error.message);}
}

function drill(aspect,platform="all"){$("review-aspect").value=aspect;$("review-platform").value=platform;$("review-score").value="low";$("review-query").value="";state.offset=0;setView("reviews");}
function setView(view){if(!names[view])view="overview";state.view=view;document.querySelectorAll(".view").forEach(el=>el.classList.toggle("active",el.id==="view-"+view));document.querySelectorAll(".nav-item").forEach(el=>el.classList.toggle("active",el.dataset.view===view));$("filter-form").classList.toggle("hidden",view==="pipeline");document.querySelector(".filter-caption").classList.toggle("hidden",view==="pipeline");$("export-summary").classList.toggle("hidden",view==="pipeline");$("crumb").textContent=names[view];$("page-title").textContent=headings[view][0];$("page-subtitle").textContent=headings[view][1];if(location.hash!=="#"+view)history.replaceState(null,"","#"+view);if(view==="reviews"&&state.data)loadReviews();window.scrollTo({top:0,behavior:"smooth"});}

async function applyFilters(){
 const platforms=[...document.querySelectorAll("#platform-filters input:checked")].map(input=>input.value);
 if(!platforms.length){toast("Chọn ít nhất một sàn để phân tích.");return;}
 const filters={platforms,from:$("from-month").value,to:$("to-month").value,exclude_spam:$("exclude-spam").checked};
 const previous=state.filters;state.filters=filters;$("filter-form").querySelector("button").disabled=true;$("error").classList.add("hidden");
 try{const result=await api("/api/overview?"+params());state.data=result;state.offset=0;renderOverview();
  $("filter-summary").textContent=`${platforms.join(" · ")} / ${filters.from} → ${filters.to}${filters.exclude_spam?" / Loại ứng viên spam":""}`;
  const selected=$("review-platform").value;$("review-platform").innerHTML='<option value="all">Các sàn đang chọn</option>'+platforms.map(p=>`<option value="${p}">${p}</option>`).join("");$("review-platform").value=platforms.includes(selected)?selected:"all";
  if(state.view==="reviews")loadReviews();
 }catch(error){state.filters=previous;showError(error);}finally{$("filter-form").querySelector("button").disabled=false;}
}

function exportChart(id){const chart=$(id).querySelector("svg");if(!chart){toast("Chưa có biểu đồ để xuất.");return;}const [,,w,h]=chart.getAttribute("viewBox").split(" ").map(Number);const titles={"trend-chart":"Xu hướng theo thời gian","rating-chart":"Phân bố số sao"};let header=text(18,27,titles[id]||"Kết quả phân tích",'font-size="17" fill="#294e5d"')+text(18,48,state.filters.platforms.join(" · ")+" / "+state.filters.from+" → "+state.filters.to,'font-size="11"');if(id==="trend-chart")header+=state.filters.platforms.map((p,i)=>`<circle cx="${22+i*95}" cy="67" r="3.5" fill="${colors[p]}"/>`+text(31+i*95,70,p)).join("");const inside=header+`<g transform="translate(0 86)">${chart.innerHTML}</g>`+text(18,h+112,"Google Play, 2013–08/2022 · Sao thấp là tín hiệu thay thế; chưa xác thực cảm xúc theo khía cạnh.",'font-size="9"');const file=svg(w,h+130,inside,titles[id]||"Kết quả");const blob=new Blob([file],{type:"image/svg+xml;charset=utf-8"});const url=URL.createObjectURL(blob),link=document.createElement("a");link.href=url;link.download=`reviewlens-${id}-${state.filters.from}-${state.filters.to}.svg`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);toast("Đã tải biểu đồ SVG kèm phạm vi dữ liệu.");}

async function initialize(){
 $("loading").classList.remove("hidden");$("error").classList.add("hidden");
 try{state.boot=await api("/api/bootstrap");const m=state.boot.metadata;
  $("sidebar-count").textContent=num(m.processed_rows);
  $("platform-filters").innerHTML=state.boot.platforms.map(p=>`<label class="platform-chip" style="--color:${colors[p]};--tint:${tints[p]}"><input type="checkbox" value="${p}" checked aria-label="Chọn ${p}"><span><i></i>${p}</span></label>`).join("");
  for(const id of ["from-month","to-month"]){$(id).min=m.month_min;$(id).max=m.month_max;}
  $("from-month").value=m.month_min;$("to-month").value=m.month_max;
  $("review-aspect").innerHTML='<option value="all">Tất cả khía cạnh</option><option value="unmatched">Chưa gán khía cạnh</option>'+m.aspects.map(a=>`<option value="${a.code}">${esc(a.label)}</option>`).join("");
  $("snapshot-date").textContent="Snapshot: "+new Intl.DateTimeFormat("vi-VN",{dateStyle:"short",timeStyle:"short",timeZone:"Asia/Ho_Chi_Minh"}).format(new Date(m.prepared_utc));
  state.filters={platforms:state.boot.platforms,from:m.month_min,to:m.month_max,exclude_spam:false};renderPipeline();await applyFilters();$("content").classList.remove("hidden");setView(location.hash.slice(1)||"overview");
 }catch(error){showError(error);}finally{$("loading").classList.add("hidden");}
}

document.querySelectorAll("[data-view]").forEach(button=>button.onclick=()=>setView(button.dataset.view));
document.querySelectorAll("[data-goto]").forEach(button=>button.onclick=()=>setView(button.dataset.goto));
$("filter-form").onsubmit=event=>{event.preventDefault();applyFilters();};
$("review-form").onsubmit=event=>{event.preventDefault();state.offset=0;loadReviews();};
$("previous-page").onclick=()=>{state.offset=Math.max(0,state.offset-20);loadReviews();};
$("next-page").onclick=()=>{state.offset+=20;loadReviews();};
$("show-unmatched").onclick=()=>drill("unmatched");
$("granularity").onchange=renderTrend;
$("trend-mode").querySelectorAll("button").forEach(button=>button.onclick=()=>{state.trend=button.dataset.mode;$("trend-mode").querySelectorAll("button").forEach(b=>b.classList.toggle("active",b===button));renderTrend();});
document.querySelectorAll("[data-chart]").forEach(button=>button.onclick=()=>exportChart(button.dataset.chart));
$("export-summary").onclick=()=>{if(state.data)window.location.href="/api/export.csv?"+params();};
$("export-reviews").onclick=()=>{if(state.data)window.location.href="/api/export-reviews.csv?"+reviewParams();};
$("print").onclick=()=>window.print();$("refresh").onclick=initialize;
window.addEventListener("hashchange",()=>setView(location.hash.slice(1)));
initialize();
