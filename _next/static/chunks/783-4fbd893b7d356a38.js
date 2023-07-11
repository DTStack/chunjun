"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[783],{3783:function(a,b,c){c.d(b,{m:function(){return aF}});var d=c(7294),e=c(8495),f=c(8216);let g={context:"Tabs component was not found in the tree",value:"Tabs.Tab or Tabs.Panel component was rendered with invalid value or without value"},[h,i]=(0,f.R)(g.context);var j=c(6817);let k={left:"flex-start",center:"center",right:"flex-end",apart:"space-between"};(0,j.k)((a,{spacing:b,position:c,noWrap:d,grow:e,align:f,count:g})=>({root:{boxSizing:"border-box",display:"flex",flexDirection:"row",alignItems:f||"center",flexWrap:d?"nowrap":"wrap",justifyContent:k[c],gap:a.fn.size({size:b,sizes:a.spacing}),"& > *":{boxSizing:"border-box",maxWidth:e?`calc(${100/g}% - ${a.fn.size({size:b,sizes:a.spacing})-a.fn.size({size:b,sizes:a.spacing})/g}px)`:void 0,flexGrow:e?1:0}}}));var l=Object.defineProperty,m=Object.getOwnPropertySymbols,n=Object.prototype.hasOwnProperty,o=Object.prototype.propertyIsEnumerable,p=(a,b,c)=>b in a?l(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,q=(a,b)=>{for(var c in b||(b={}))n.call(b,c)&&p(a,c,b[c]);if(m)for(var c of m(b))o.call(b,c)&&p(a,c,b[c]);return a},r=(0,j.k)((a,b)=>{let c="vertical"===b.orientation;return{tabsList:q({display:"flex",flexWrap:"wrap",flexDirection:c?"column":"row",justifyContent:k[b.position],'& [role="tab"]':{flex:b.grow?1:void 0}},function({variant:a,orientation:b,inverted:c,placement:d},e){let f="vertical"===b;return"default"===a?{[f?"left"===d?"borderRight":"borderLeft":c?"borderTop":"borderBottom"]:`2px solid ${"dark"===e.colorScheme?e.colors.dark[4]:e.colors.gray[3]}`}:"outline"===a?{[f?"left"===d?"borderRight":"borderLeft":c?"borderTop":"borderBottom"]:`1px solid ${"dark"===e.colorScheme?e.colors.dark[4]:e.colors.gray[3]}`}:"pills"===a?{gap:`calc(${e.spacing.sm}px / 2)`}:{}}(b,a))}}),s=c(7414),t=Object.defineProperty,u=Object.defineProperties,v=Object.getOwnPropertyDescriptors,w=Object.getOwnPropertySymbols,x=Object.prototype.hasOwnProperty,y=Object.prototype.propertyIsEnumerable,z=(a,b,c)=>b in a?t(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,A=(a,b)=>{for(var c in b||(b={}))x.call(b,c)&&z(a,c,b[c]);if(w)for(var c of w(b))y.call(b,c)&&z(a,c,b[c]);return a},B=(a,b)=>u(a,v(b)),C=(a,b)=>{var c={};for(var d in a)x.call(a,d)&&0>b.indexOf(d)&&(c[d]=a[d]);if(null!=a&&w)for(var d of w(a))0>b.indexOf(d)&&y.call(a,d)&&(c[d]=a[d]);return c};let D={grow:!1,position:"left"},E=(0,d.forwardRef)((a,b)=>{let c=(0,e.N4)("TabsList",D,a),{children:f,className:g,grow:h,position:j}=c,k=C(c,["children","className","grow","position"]),{orientation:l,variant:m,color:n,radius:o,inverted:p,placement:q,classNames:t,styles:u,unstyled:v}=i(),{classes:w,cx:x}=r({orientation:l,grow:h,variant:m,color:n,position:j,radius:o,inverted:p,placement:q},{name:"Tabs",unstyled:v,classNames:t,styles:u});return d.createElement(s.x,B(A({},k),{className:x(w.tabsList,g),ref:b,role:"tablist","aria-orientation":l}),f)});E.displayName="@mantine/core/TabsList";var F=c(7818),G=(0,j.k)((a,{orientation:b})=>({panel:{flex:"vertical"===b?1:void 0}})),H=Object.defineProperty,I=Object.defineProperties,J=Object.getOwnPropertyDescriptors,K=Object.getOwnPropertySymbols,L=Object.prototype.hasOwnProperty,M=Object.prototype.propertyIsEnumerable,N=(a,b,c)=>b in a?H(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,O=(a,b)=>{for(var c in b||(b={}))L.call(b,c)&&N(a,c,b[c]);if(K)for(var c of K(b))M.call(b,c)&&N(a,c,b[c]);return a},P=(a,b)=>I(a,J(b)),Q=(a,b)=>{var c={};for(var d in a)L.call(a,d)&&0>b.indexOf(d)&&(c[d]=a[d]);if(null!=a&&K)for(var d of K(a))0>b.indexOf(d)&&M.call(a,d)&&(c[d]=a[d]);return c};let R={},S=(0,d.forwardRef)((a,b)=>{let c=(0,e.N4)("TabsPanel",R,a),{value:f,children:g,sx:h,className:j}=c,k=Q(c,["value","children","sx","className"]),l=i(),{classes:m,cx:n}=G({orientation:l.orientation,variant:l.variant,color:l.color,radius:l.radius,inverted:l.inverted,placement:l.placement},{name:"Tabs",unstyled:l.unstyled,classNames:l.classNames,styles:l.styles}),o=l.value===f,p=l.keepMounted?g:o?g:null;return d.createElement(s.x,P(O({},k),{ref:b,sx:[{display:o?void 0:"none"},...(0,F.R)(h)],className:n(m.panel,j),role:"tabpanel",id:l.getPanelId(f),"aria-labelledby":l.getTabId(f)}),p)});S.displayName="@mantine/core/TabsPanel";var T=c(6650),U=Object.defineProperty,V=Object.defineProperties,W=Object.getOwnPropertyDescriptors,X=Object.getOwnPropertySymbols,Y=Object.prototype.hasOwnProperty,Z=Object.prototype.propertyIsEnumerable,$=(a,b,c)=>b in a?U(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,_=(a,b)=>{for(var c in b||(b={}))Y.call(b,c)&&$(a,c,b[c]);if(X)for(var c of X(b))Z.call(b,c)&&$(a,c,b[c]);return a},aa=(a,b)=>V(a,W(b)),ab=(0,j.k)((a,b)=>({tabLabel:{},tab:_({position:"relative",padding:`${a.spacing.xs}px ${a.spacing.md}px`,paddingLeft:b.withIcon?a.spacing.xs:void 0,paddingRight:b.withRightSection?a.spacing.xs:void 0,fontSize:a.fontSizes.sm,whiteSpace:"nowrap",zIndex:0,display:"flex",alignItems:"center",justifyContent:"horizontal"===b.orientation?"center":void 0,lineHeight:1,"&:disabled":_({opacity:.5,cursor:"not-allowed"},a.fn.hover({backgroundColor:"transparent"})),"&:focus":{zIndex:1}},function(a,{variant:b,orientation:c,color:d,radius:e,inverted:f,placement:g}){let h="vertical"===c,i=a.fn.variant({color:d,variant:"filled"}),j=a.fn.radius(e),k="vertical"===c?"left"===g?`${j}px 0 0 ${j}px`:` 0 ${j}px ${j}px 0`:f?`0 0 ${j}px ${j}px`:`${j}px ${j}px 0 0`;return"default"===b?aa(_({[h?"left"===g?"borderRight":"borderLeft":f?"borderTop":"borderBottom"]:"2px solid transparent",[h?"left"===g?"marginRight":"marginLeft":f?"marginTop":"marginBottom"]:-2,borderRadius:k},a.fn.hover({backgroundColor:"dark"===a.colorScheme?a.colors.dark[6]:a.colors.gray[0],borderColor:"dark"===a.colorScheme?a.colors.dark[4]:a.colors.gray[3]})),{"&[data-active]":_({borderColor:i.background,color:"dark"===a.colorScheme?a.white:a.black},a.fn.hover({borderColor:i.background}))}):"outline"===b?{borderRadius:k,border:"1px solid transparent",[h?"left"===g?"borderRight":"borderLeft":f?"borderTop":"borderBottom"]:"none","&[data-active]":{borderColor:"dark"===a.colorScheme?a.colors.dark[4]:a.colors.gray[3],"&::before":{content:'""',backgroundColor:"dark"===a.colorScheme?a.colors.dark[7]:a.white,position:"absolute",bottom:h?0:f?"unset":-1,top:h?0:f?-1:"unset",[h?"width":"height"]:1,right:h?"left"===g?-1:"unset":0,left:h?"left"===g?"unset":-1:0}}}:"pills"===b?aa(_({borderRadius:a.fn.radius(e)},a.fn.hover({backgroundColor:"dark"===a.colorScheme?a.colors.dark[6]:a.colors.gray[0]})),{"&[data-active]":_({backgroundColor:i.background,color:a.white},a.fn.hover({backgroundColor:i.background}))}):{}}(a,b)),tabRightSection:{display:"flex",justifyContent:"center",alignItems:"center","&:not(:only-child)":{marginLeft:7}},tabIcon:{display:"flex",justifyContent:"center",alignItems:"center","&:not(:only-child)":{marginRight:7}}})),ac=c(4736),ad=Object.defineProperty,ae=Object.defineProperties,af=Object.getOwnPropertyDescriptors,ag=Object.getOwnPropertySymbols,ah=Object.prototype.hasOwnProperty,ai=Object.prototype.propertyIsEnumerable,aj=(a,b,c)=>b in a?ad(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,ak=(a,b)=>{for(var c in b||(b={}))ah.call(b,c)&&aj(a,c,b[c]);if(ag)for(var c of ag(b))ai.call(b,c)&&aj(a,c,b[c]);return a},al=(a,b)=>ae(a,af(b)),am=(a,b)=>{var c={};for(var d in a)ah.call(a,d)&&0>b.indexOf(d)&&(c[d]=a[d]);if(null!=a&&ag)for(var d of ag(a))0>b.indexOf(d)&&ai.call(a,d)&&(c[d]=a[d]);return c};let an={},ao=(0,d.forwardRef)((a,b)=>{let c=(0,e.N4)("TabsTab",an,a),{value:f,children:g,onKeyDown:h,onClick:j,className:k,icon:l,rightSection:m,color:n}=c,o=am(c,["value","children","onKeyDown","onClick","className","icon","rightSection","color"]),p=i(),q=!!l,r=!!m,{theme:s,classes:t,cx:u}=ab({withIcon:q||r&&!g,withRightSection:r||q&&!g,orientation:p.orientation,color:n||p.color,variant:p.variant,radius:p.radius,inverted:p.inverted,placement:p.placement},{name:"Tabs",unstyled:p.unstyled,classNames:p.classNames,styles:p.styles}),v=f===p.value,w=a=>{p.onTabChange(p.allowTabDeactivation&&f===p.value?null:f),null==j||j(a)};return d.createElement(ac.k,al(ak({},o),{unstyled:p.unstyled,className:u(t.tab,k),"data-active":v||void 0,ref:b,type:"button",role:"tab",id:p.getTabId(f),"aria-selected":v,tabIndex:v||null===p.value?0:-1,"aria-controls":p.getPanelId(f),onClick:w,onKeyDown:(0,T.R)({siblingSelector:'[role="tab"]',parentSelector:'[role="tablist"]',activateOnFocus:p.activateTabWithKeyboard,loop:p.loop,dir:s.dir,orientation:p.orientation,onKeyDown:h})}),l&&d.createElement("div",{className:t.tabIcon},l),g&&d.createElement("div",{className:t.tabLabel},g),m&&d.createElement("div",{className:t.tabRightSection},m))});function ap(a,b){return c=>{if("string"!=typeof c||0===c.trim().length)throw Error(b);return`${a}-${c}`}}ao.displayName="@mantine/core/Tab";var aq=c(6289),ar=c(5851);function as({defaultValue:a,value:b,onTabChange:c,orientation:e,children:f,loop:i,id:j,activateTabWithKeyboard:k,allowTabDeactivation:l,variant:m,color:n,radius:o,inverted:p,placement:q,keepMounted:r=!0,classNames:s,styles:t,unstyled:u}){let v=(0,aq.M)(j),[w,x]=(0,ar.C)({value:b,defaultValue:a,finalValue:null,onChange:c});return d.createElement(h,{value:{placement:q,value:w,orientation:e,id:v,loop:i,activateTabWithKeyboard:k,getTabId:ap(`${v}-tab`,g.value),getPanelId:ap(`${v}-panel`,g.value),onTabChange:x,allowTabDeactivation:l,variant:m,color:n,radius:o,inverted:p,keepMounted:r,classNames:s,styles:t,unstyled:u}},f)}as.displayName="@mantine/core/TabsProvider";var at=(0,j.k)((a,{orientation:b,placement:c})=>({root:{display:"vertical"===b?"flex":void 0,flexDirection:"right"===c?"row-reverse":"row"}})),au=Object.defineProperty,av=Object.defineProperties,aw=Object.getOwnPropertyDescriptors,ax=Object.getOwnPropertySymbols,ay=Object.prototype.hasOwnProperty,az=Object.prototype.propertyIsEnumerable,aA=(a,b,c)=>b in a?au(a,b,{enumerable:!0,configurable:!0,writable:!0,value:c}):a[b]=c,aB=(a,b)=>{for(var c in b||(b={}))ay.call(b,c)&&aA(a,c,b[c]);if(ax)for(var c of ax(b))az.call(b,c)&&aA(a,c,b[c]);return a},aC=(a,b)=>av(a,aw(b)),aD=(a,b)=>{var c={};for(var d in a)ay.call(a,d)&&0>b.indexOf(d)&&(c[d]=a[d]);if(null!=a&&ax)for(var d of ax(a))0>b.indexOf(d)&&az.call(a,d)&&(c[d]=a[d]);return c};let aE={orientation:"horizontal",loop:!0,activateTabWithKeyboard:!0,allowTabDeactivation:!1,unstyled:!1,inverted:!1,variant:"default",placement:"left"},aF=(0,d.forwardRef)((a,b)=>{let c=(0,e.N4)("Tabs",aE,a),{defaultValue:f,value:g,orientation:h,loop:i,activateTabWithKeyboard:j,allowTabDeactivation:k,children:l,id:m,onTabChange:n,variant:o,color:p,className:q,unstyled:r,classNames:t,styles:u,radius:v,inverted:w,keepMounted:x,placement:y}=c,z=aD(c,["defaultValue","value","orientation","loop","activateTabWithKeyboard","allowTabDeactivation","children","id","onTabChange","variant","color","className","unstyled","classNames","styles","radius","inverted","keepMounted","placement"]),{classes:A,cx:B}=at({orientation:h,color:p,variant:o,radius:v,inverted:w,placement:y},{unstyled:r,name:"Tabs",classNames:t,styles:u});return d.createElement(as,{activateTabWithKeyboard:j,defaultValue:f,orientation:h,onTabChange:n,value:g,id:m,loop:i,allowTabDeactivation:k,color:p,variant:o,radius:v,inverted:w,keepMounted:x,placement:y,classNames:t,styles:u,unstyled:r},d.createElement(s.x,aC(aB({},z),{className:B(A.root,q),id:m,ref:b}),l))});aF.List=E,aF.Tab=ao,aF.Panel=S,aF.displayName="@mantine/core/Tabs"}}])