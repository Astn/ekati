window.d3Interop = {
    drag: function (simulation) {

        function dragstarted(d) {
            if (!d3.event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }

        function dragged(d) {
            d.fx = d3.event.x;
            d.fy = d3.event.y;
        }

        function dragended(d) {
            if (!d3.event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    },
    dragViewPort: function dragViewPort(simulation, svg){
        function dragstarted(d) {
            //if (!d3.event.active) simulation.alphaTarget(0.3).restart();
        }

        function dragged(d) {
            this.viewBox.baseVal.x -= d3.event.dx;
            this.viewBox.baseVal.y -= d3.event.dy;
        }

        function dragended(d) {
            //if (!d3.event.active) simulation.alphaTarget(0);
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    },
    zoomViewPort: function zoomViewPort(simulation, svg){
        function dragstarted(d) {
            //if (!d3.event.active) simulation.alphaTarget(0.3).restart();
        }

        function zoom(d) {
            var factor = d3.event.sourceEvent.deltaY / 100; // get percent change -0.03
            if(isNaN(factor) || factor === Infinity || factor === -Infinity){
                return;
            }
            var widthChange = (this.viewBox.baseVal.width * factor);
            var heightChange = (this.viewBox.baseVal.height * factor);
            this.viewBox.baseVal.x -= widthChange / 2;
            this.viewBox.baseVal.y -= heightChange / 2;
            this.viewBox.baseVal.width += widthChange;
            this.viewBox.baseVal.height += heightChange;
        }

        function dragended(d) {
            //if (!d3.event.active) simulation.alphaTarget(0);
        }

        return d3.zoom()
            .on("start", dragstarted)
            .on("zoom", zoom)
            .on("end", dragended);
    },
    linkArk: function (d) {
        const r = Math.hypot(d.target.x - d.source.x, d.target.y - d.source.y);
        return `
    M${d.source.x},${d.source.y}
    A${r},${r} 0 0,1 ${d.target.x},${d.target.y}
  `;
    },
  toReScale: function toReScale(inputY){
      xMax = 30;
      xMin = 4;

      yMax = 1000;
      yMin = 0;

      percent = (inputY - yMin) / (yMax - yMin);
      return percent * (xMax - xMin) + xMin;
  },
  renderGraphInternal:  function renderGraphInternal(elementid, data) {
      if(data.length === 0){
          d3.selectAll("svg > *").remove();
          return ;
      }

      var ctr = 0;
      var width = 10000, height = 10000
      var viewBoxScale = this.toReScale(data.length ); // full size is 100
      var deltax = 0;
      var deltay = 0;
      var panx = 0;
      var pany = 0;
      var clickX = 0;
      var clickY = 0;
      var centerX=width/2 + panx;
      var centerY=height/2 + pany;
      var vbWidth = (width/100)  * viewBoxScale;
      var vbHeight = (height/100) * viewBoxScale;
      var topleftX = (centerX - vbWidth/2);
      var topleftY = (centerY - vbHeight/2);

      const nodes = data.map(d => {return {source:d.source}} );
      const links = data.map(d => 
          d.edges.map(typ => {
              ctr = ctr + 1;
              var fjdsk = typ.target.map(t => {
                  return {
                      "source": d.source,
                      "target": t,
                      "type" : typ.type,
                      "myid" : `${ctr}`
              }});
              return fjdsk;
          }).flat()).flat()
          .filter(l => {
              return nodes.find(n => n.source === l.target);
          });

      const types = Array.from(new Set(links.map(d => d.type)));
      const color = d3.scaleOrdinal(types, d3.schemeCategory10);
      const forceL = d3.forceLink(links).id(d => d.source);

      const simulation = d3.forceSimulation(nodes)
          .force("link", forceL )
          .force("charge", d3.forceManyBody().strength(-800))
          .force("x", d3.forceX(centerX))
          .force("y", d3.forceY(centerY))    ;
          //.force("x", d3.forceX())
          //.force("y", d3.forceY());

      d3.selectAll("svg > *").remove();
      const svg = d3.select("svg")
          .attr("viewBox", [topleftX,topleftY, vbWidth, vbHeight])
          .style("font", "8px sans-serif")
          .style("fill", "#686868")
          .style("background-color","#181818");
      
          //.attr("viewBox", [-width , -height , width*2, height*2])
      function clicked() {
          if(d3.event.sourceEvent.type === "wheel") return false;
          clickX = d3.event.sourceEvent.layerX;
          clickY = d3.event.sourceEvent.layerY;
          return true;
      }
      // attach Pan.
      svg.call(this.dragViewPort(simulation), svg);
      svg.call(this.zoomViewPort(simulation));
      
      console.log("renderGraph:3",svg);
      
      
      // Per-type markers, as they don't inherit styles.
      svg.append("defs").selectAll("marker")
          .data(types)
          .join("marker")
          .attr("id", d => `arrow-${d}`)
          .attr("viewBox", "0 -5 10 10")
          .attr("refX", 15)
          .attr("refY", -0.5)
          .attr("markerWidth", 6)
          .attr("markerHeight", 6)
          .attr("orient", "auto")
          .append("path")
          .attr("fill", color)
          .attr("d", "M0,-5L10,0L0,5");

      const g1 = svg.append("g");
      const link = g1
          .attr("fill", "none")
          .attr("stroke-width", 1.5)
          .selectAll("path")
          .data(links)
          .join("path")
          .attr("id",d => `tp${d.myid}`)
          .attr("stroke", d => color(d.type))
          .attr("marker-end", d => `url(#arrow-${d.type})`);
      
      svg.append("g").selectAll("text")
          .data(links)
          .join("text")
          .append("textPath")
          .attr("fill", "#787878")
          .attr("startOffset", "25%")
          .attr("stroke", "none")
          .attr("href", d =>`${location.href}#tp${d.myid}`)
          .text(d => d.type);
          

      const node = svg.append("g")
          .attr("fill", "currentColor")
          .attr("stroke-linecap", "round")
          .attr("stroke-linejoin", "round")
          .selectAll("g")
          .data(nodes)
          .join("g")
          .call(this.drag(simulation));

      node.append("circle")
          .attr("stroke", "black")
          .attr("stroke-width", 1.5)
          .attr("r", 4);

      node.append("text")
          .attr("x", 8)
          .attr("y", "0.31em")
          .text(d => d.source)
          .attr("fill", "#888888")
          .attr("stroke", "none");

      const lar = this.linkArk;
      
      simulation.on("tick", () => {
          link.attr("d", (x) => lar(x));
          node.attr("transform", d => `translate(${d.x},${d.y})`);
      });

      invalidation.then(() => simulation.stop());

      return svg.node();
  },
  renderGraph: function renderGreaph(elementid, data) {
    setTimeout(function(){
            window.d3Interop.renderGraphInternal(elementid, data);
        });
  }
};

