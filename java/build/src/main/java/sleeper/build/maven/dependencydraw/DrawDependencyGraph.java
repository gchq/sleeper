/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.build.maven.dependencydraw;

import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.DefaultModalGraphMouse;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer;

import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ItemEvent;

public class DrawDependencyGraph {
    public boolean showTransitiveDependencies = false;

    public void drawGraph(GraphModel model) {
        DirectedGraph<GraphNode, GraphEdge> g = new DirectedSparseGraph<>();
        model.getNodes().forEach(g::addVertex);
        model.getEdges().forEach(edge -> g.addEdge(edge, edge.getFrom(model), edge.getTo(model)));
        KKLayout<GraphNode, GraphEdge> layout = new KKLayout<>(g);
        layout.setLengthFactor(1.2);
        layout.setSize(new Dimension(900, 900));

        JFrame frame = new JFrame("Dependency Graph View");
        VisualizationViewer<GraphNode, GraphEdge> vv = new VisualizationViewer<>(layout);
        PickedNodeState picked = new PickedNodeState(model);
        vv.getPickedVertexState().addItemListener(event ->
                picked.updatePicked(vv.getPickedVertexState().getPicked()));
        vv.setPreferredSize(new Dimension(950, 950));
        vv.getRenderContext().setEdgeDrawPaintTransformer(edge -> picked.calculateEdgeColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowDrawPaintTransformer(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowFillPaintTransformer(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller());
        vv.getRenderContext().setVertexDrawPaintTransformer(node -> new Color(255, 255, 255, 0));
        vv.getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);
        vv.setVertexToolTipTransformer(new ToStringLabeller());
        vv.setEdgeToolTipTransformer(new ToStringLabeller());

        DefaultModalGraphMouse<GraphNode, GraphEdge> gm = new DefaultModalGraphMouse<>();
        vv.setGraphMouse(gm);
        vv.addKeyListener(gm.getModeKeyListener());
        gm.setMode(ModalGraphMouse.Mode.TRANSFORMING);

        JLabel text = new JLabel("P - Selection Mode | T - Traverse mode |\n");
        JLabel text2 = new JLabel("Red - Going to | Blue - Going from");
        JCheckBox transitiveCheckBox = new JCheckBox("Show transitive dependencies");
        transitiveCheckBox.addItemListener(e -> {
            showTransitiveDependencies = e.getStateChange() == ItemEvent.SELECTED;
            SwingUtilities.updateComponentTreeUI(frame);
        });
        vv.add(text, BorderLayout.CENTER);
        vv.add(text2, BorderLayout.CENTER);
        vv.add(transitiveCheckBox);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv);
        frame.pack();
        frame.setVisible(true);
    }
}
