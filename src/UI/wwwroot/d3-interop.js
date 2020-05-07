window.d3Interop = {
    drag: function (worker) {
        const wak = worker;
        function dragstarted(d) {
            d.fx = d.x;
            d.fy = d.y;

            // Post data to worker
            wak.postMessage({
                type: 'dragged',
                dragged: {
                    fx: d.fx,
                    fy: d.fy
                },
            });
            
        }

        function dragged(d) {
            d.fx = d3.event.x;
            d.fy = d3.event.y;
            // Post data to worker
            wak.postMessage({
                type: 'dragged',
                dragged: {
                    fx: d.fx,
                    fy: d.fy
                },
            });
        }

        function dragended(d) {
            d.fx = null;
            d.fy = null;
            // Post data to worker
            wak.postMessage({
                type: 'dragged',
                dragged: {
                    fx: d.fx,
                    fy: d.fy
                },
            });
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    },
    dragViewPort: function dragViewPort(){
        function dragstarted(d) {
        }

        function dragged(d) {
            this.viewBox.baseVal.x -= d3.event.dx;
            this.viewBox.baseVal.y -= d3.event.dy;
        }

        function dragended(d) {
        }

        return d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    },
    zoomViewPort: function zoomViewPort(){
        var desiredZoom = 0;
        function dragstarted(d) {
        }

        function zoom(d) {
            if(d3.event.sourceEvent.deltaY > 0 && desiredZoom < 0 
                || d3.event.sourceEvent.deltaY < 0 && desiredZoom > 0){
                desiredZoom = 0;
                desiredZoom += d3.event.sourceEvent.deltaY;
            } else {
                desiredZoom += d3.event.sourceEvent.deltaY;
            }
            var factor = desiredZoom / 100; // get percent change -0.03
            if(isNaN(factor) || factor === Infinity || factor === -Infinity){
                return;
            }
            var widthChange = (this.viewBox.baseVal.width * factor);
            var heightChange = (this.viewBox.baseVal.height * factor);

            const x = this.viewBox.baseVal.x - widthChange / 2;
            const y = this.viewBox.baseVal.y - heightChange / 2;
            const w =  this.viewBox.baseVal.width + widthChange;
            const h = this.viewBox.baseVal.height + heightChange;
            
            
            d3.select("svg")
                .transition()           
                .ease(d3.easePoly)        
                .duration(1000)           
                .attr('viewBox', x +' '+ y +' '+ w +' '+h)
                .each('end', () => {
                    desiredZoom = 0;
                });     
               
            
            
        }

        function dragended(d) {
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
          xMax = 40;
          xMin = 4;
    
          yMax = 1000;
          yMin = 0;
    
          percent = (inputY - yMin) / (yMax - yMin);
          return percent * (xMax - xMin) + xMin;
      },
    worker : undefined,
    showNodes: function showNodes(showThem){
        if(showThem){
            d3.selectAll("svg > g > g > circle").style("display","inline");
        }else{
            d3.selectAll("svg > g > g > circle").style("display","none");
        }
    },
    showEdges: function showEdges(showThem){
        if(showThem){
            d3.select("svg > g ").style("display","inline");
        }else{
            d3.select("svg > g ").style("display","none");
        }
    },
    showEdgeLabels: function showEdgeLabels(showThem){
        if(showThem){
            d3.selectAll("svg > g > text").style("display","inline");
        }else{
            d3.selectAll("svg > g > text").style("display","none");
        }
    },
    showNodeLabels: function showNodeLabels(showThem) {
        if(showThem){
            d3.selectAll("svg > g > g > text").style("display","inline");
        }else{
            d3.selectAll("svg > g > g > text").style("display","none");
        }
    },
    pauseLayoutInternal:false,
    pauseLayout: function pauseLayout(pause){
        // avoid strange things if they unpause while its already running.
      if(this.pauseLayoutInternal !== pause){
          this.pauseLayoutInternal = pause;
          
          // posting this update message when we are already running will cause "bad things"
          if(this.pauseLayoutInternal === false){
              this.worker.postMessage({
                  type: 'update'
              })
          }
      }
    },
  renderGraphInternal:  function renderGraphInternal(elementid, data) {
      if(data.length === 0){
          d3.selectAll("svg > *").remove();
          return ;
      }
      d3.selectAll("svg > *").remove();
      
      // start over
      if(this.worker === undefined){
        this.worker = new Worker('d3worker.js');
      }




      var width = 10000, height = 10000
      var viewBoxScale = this.toReScale(data.length ); // full size is 100
      var centerX=width/2 ;
      var centerY=height/2;
      var vbWidth = (width/100)  * viewBoxScale;
      var vbHeight = (height/100) * viewBoxScale;
      var topleftX = (centerX - vbWidth/2);
      var topleftY = (centerY - vbHeight/2);

      var ctr = 0;
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
      
      
      // Post data to worker
      this.worker.postMessage({
          type: 'initial',
          nodes: nodes,
          links: links,
          width: width,
          height: height,
      });

      const throttledUpdate = _.throttle((event) => {
          switch (event.data.type) {
              case 'update': update(event.data); break
              case 'initial': initial(event.data, this.worker); break
          }

          // note that to get this booted up again its done in the function that sets pauseLayoutInternal (pauseLayout)
          if(this.pauseLayoutInternal === false){
              this.worker.postMessage({
                  type: 'update'
              }); 
          }
          
      }, 66)

      this.worker.onmessage = function(event) {
          requestAnimationFrame(() => throttledUpdate(event))
      };
      
      
      
      ////////////////////




      const lar = this.linkArk;
      
      let nodeSelection, textSelection, linkSelection
      let initialLinks, initialNodes

      function initial(data, worker) {

          // Get data with x and y values
          var links = data.links;
          var nodes = data.nodes;

          initialNodes = nodes;
          initialLinks = links;

          const svg = d3.select("svg")
              .attr("viewBox", [topleftX,topleftY, vbWidth, vbHeight])
              .style("font", "8px sans-serif")
              .style("fill", "#686868")
              .style("background-color","#181818");

          svg.call(this.d3Interop.dragViewPort());
          svg.call(this.d3Interop.zoomViewPort());

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

          var textGroup = svg.append("g");
          textGroup.selectAll("text")
              .data(links)
              .join("text")
              .style("display","none")
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
              .call(this.d3Interop.drag(worker));

          node.append("circle")
              .attr("stroke", "black")
              .attr("stroke-width", 1.5)
              .attr("r", 4);

          const text = node.append("text")
              .attr("x", 8)
              .attr("y", "0.31em")
              .text(d => d.source)
              .attr("fill", "#888888")
              .attr("stroke", "none")
              .style("display","none");

          nodeSelection = node
          textSelection = text
          linkSelection = link
      }

      function update(data) {
          var links = data.links;
          var nodes = data.nodes;

          // copy because selection has binding to initialNodes
          nodes.forEach((node, i) => {
              initialNodes[i].x = node.x
              initialNodes[i].y = node.y
          })
          
          linkSelection.attr("d", (x) => lar(x));
          nodeSelection
              .attr('transform', function(d) {
                  return 'translate(' + d.x + ',' + d.y + ')';
              })

          // linkSelection
          //     .attr("x1", function(d) { return d.source.x; })
          //     .attr("y1", function(d) { return d.source.y; })
          //     .attr("x2", function(d) { return d.target.x; })
          //     .attr("y2", function(d) { return d.target.y; });

      }
      
  },
  renderGraph: function renderGreaph(elementid, data) {
    setTimeout(function(){
            window.d3Interop.renderGraphInternal(elementid, data);
        });
  }
};

