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
    linkArk: function (d) {
        const r = Math.hypot(d.target.x - d.source.x, d.target.y - d.source.y);
        return `
    M${d.source.x},${d.source.y}
    A${r},${r} 0 0,1 ${d.target.x},${d.target.y}
  `;
    },
  renderGraph:  function renderGraph(elementid, data) {

      console.log("renderGraph",elementid,data);
      if(data.length === 0){
          d3.selectAll("svg > *").remove();
          return ;
      }
      var ctr = 0;
      var width = 300, height = 300
      const nodes = data.map(d => d.id);
      const links = data.map(d => d.attributes
          .filter(a => a.value?.data?.nodeid !== null)
          .map(a => {
              ctr = ctr +1;
              return {
                  "source": d.id.iri,
                  "target": a.value.data.nodeid.iri,
                  "type": a.key.data.str,
                  "myid": `${ctr}`
              };
          })).flat().filter(l => nodes.find(n => n.iri === l.source)  && nodes.find(n => n.iri === l.target));

      const types = Array.from(new Set(links.map(d => d.type)));
      const color = d3.scaleOrdinal(types, d3.schemeCategory10)
      const forceL = d3.forceLink(links).id(d => d.iri);

      const simulation = d3.forceSimulation(nodes)
          .force("link", forceL )
          .force("charge", d3.forceManyBody().strength(-800))
          .force("x", d3.forceX())
          .force("y", d3.forceY());

      d3.selectAll("svg > *").remove();
      const svg = d3.select("svg")
          .attr("viewBox", [-width / 2, -height / 2, width, height])
          .style("font", "8px sans-serif")
          .style("fill", "#686868")
          .style("background-color","#181818");
      
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
          .text(d => d.iri)
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
};

