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

import org.jgrapht.Graph;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.jungrapht.samples.util.LayoutHelperDirectedGraphs;
import org.jungrapht.visualization.VisualizationViewer;
import org.jungrapht.visualization.layout.algorithms.BalloonLayoutAlgorithm;
import org.jungrapht.visualization.layout.algorithms.LayoutAlgorithm;
import org.jungrapht.visualization.layout.algorithms.RadialTreeLayoutAlgorithm;
import org.jungrapht.visualization.renderers.Renderer;
import org.jungrapht.visualization.util.LayoutAlgorithmTransition;
import org.jungrapht.visualization.util.LayoutPaintable;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.SwingUtilities;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ItemEvent;

public class DrawDependencyGraph {
    public boolean showTransitiveDependencies = false;

    LayoutPaintable.BalloonRings balloonLayoutRings;
    LayoutPaintable.RadialRings radialLayoutRings;

    String instructions =
            "<html>"
                    + "<h3>Graph Transformation:</h3>"
                    + "<ul>"
                    + "<li>Mousewheel scales with a crossover value of 1.0.<p>"
                    + "     - scales the graph layout when the combined scale is greater than 1<p>"
                    + "     - scales the graph view when the combined scale is less than 1"
                    + "<li>Mouse1+drag pans the graph"
                    + "<li>Mouse1 double click on the background resets all transforms"
                    + "</ul>"
                    + "<h3>Vertex/Edge Selection:</h3>"
                    + "<ul>"
                    + "<li>Mouse1+MENU on a vertex or edge selects the vertex or edge and deselects any others"
                    + "<li>Mouse1+MENU+Shift on a vertex toggles selection of the vertex"
                    + "<li>Mouse1+MENU on a selected edge toggles selection of the edge"
                    + "<li>Mouse1+MENU+drag elsewhere selects vertices in a region"
                    + "<li>Mouse1+Shift+drag adds selection of vertices in a new region"
                    + "</ul>"
                    + "<h3>Vertex Transformation:</h3>"
                    + "<ul>"
                    + "<li>Mouse1+MENU+drag on a selected vertex moves all selected Vertices"
                    + "</ul>"
                    + "Note that MENU == Command on a Mac, MENU == CTRL on a PC"
                    + "</html>";

    public void drawGraph(GraphModel model) {
        Graph<GraphNode, GraphEdge> g =
                GraphTypeBuilder.<GraphNode, GraphEdge>directed().buildGraph();
        model.getNodes().forEach(g::addVertex);
        model.getEdges().forEach(edge -> g.addEdge(edge.getFrom(model), edge.getTo(model), edge));

        Dimension size = new Dimension(600, 600);
        VisualizationViewer<GraphNode, GraphEdge> vv =
                VisualizationViewer.builder(g)
                        .viewSize(size)
                        .layoutSize(size).build();

        // use html to break long labels into multi-line and center-align the text
        vv.getRenderContext().setVertexLabelFunction(v -> "<html><b><center>" +
                v.toString().replaceAll("/", "/<br>"));

        PickedNodeState picked = new PickedNodeState(model);
        vv.getRenderContext().getSelectedVertexState().addItemListener(event ->
                picked.updatePicked(vv.getRenderContext().getSelectedVertexState().getSelected()));
        vv.getRenderContext().setEdgeDrawPaintFunction(edge -> picked.calculateEdgeColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowDrawPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowFillPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setVertexDrawPaintFunction(node -> new Color(255, 255, 255, 0));
        vv.getRenderContext().setVertexLabelPosition(Renderer.VertexLabel.Position.S);
        vv.setVertexToolTipFunction(Object::toString);
        vv.setEdgeToolTipFunction(Object::toString);

        LayoutHelperDirectedGraphs.Layouts[] combos = LayoutHelperDirectedGraphs.getCombos();
        final JRadioButton animateLayoutTransition = new JRadioButton("Animate Transition", true);

        final JComboBox layoutComboBox = new JComboBox(combos);
        layoutComboBox.addActionListener(
                e ->
                        SwingUtilities.invokeLater(
                                () -> {
                                    LayoutHelperDirectedGraphs.Layouts layoutBuilderType =
                                            (LayoutHelperDirectedGraphs.Layouts) layoutComboBox.getSelectedItem();
                                    LayoutAlgorithm.Builder layoutAlgorithmBuilder =
                                            layoutBuilderType.getLayoutAlgorithmBuilder();
                                    LayoutAlgorithm<GraphNode> layoutAlgorithm = layoutAlgorithmBuilder.build();
                                    vv.removePreRenderPaintable(balloonLayoutRings);
                                    vv.removePreRenderPaintable(radialLayoutRings);
                                    layoutAlgorithm.setAfter(vv::scaleToLayout);
                                    if (animateLayoutTransition.isSelected()) {
                                        LayoutAlgorithmTransition.animate(vv, layoutAlgorithm, vv::scaleToLayout);
                                    } else {
                                        LayoutAlgorithmTransition.apply(vv, layoutAlgorithm, vv::scaleToLayout);
                                    }
                                    if (layoutAlgorithm instanceof BalloonLayoutAlgorithm) {
                                        balloonLayoutRings =
                                                new LayoutPaintable.BalloonRings(
                                                        vv, (BalloonLayoutAlgorithm) layoutAlgorithm);
                                        vv.addPreRenderPaintable(balloonLayoutRings);
                                    }
                                    if (layoutAlgorithm instanceof RadialTreeLayoutAlgorithm) {
                                        radialLayoutRings =
                                                new LayoutPaintable.RadialRings(
                                                        vv, (RadialTreeLayoutAlgorithm) layoutAlgorithm);
                                        vv.addPreRenderPaintable(radialLayoutRings);
                                    }
                                 }));

        layoutComboBox.setSelectedItem(LayoutHelperDirectedGraphs.Layouts.SUGIYAMA);

        JFrame frame = new JFrame("Dependency Graph View");
        JPanel controlPanel = new JPanel();
        JLabel text2 = new JLabel("Red - Going to | Blue - Going from");
        JButton help = new JButton("?");
        help.addActionListener(e -> {
            // make a non-modal dialog with instructions
            JOptionPane pane = new JOptionPane(instructions);
            JDialog dialog = pane.createDialog(frame, "Help");
            dialog.setModal(false);
            dialog.show();
        });

        JCheckBox transitiveCheckBox = new JCheckBox("Show transitive dependencies");
        transitiveCheckBox.addItemListener(e -> {
            showTransitiveDependencies = e.getStateChange() == ItemEvent.SELECTED;
            SwingUtilities.updateComponentTreeUI(frame);
        });
        controlPanel.add(layoutComboBox);
        controlPanel.add(animateLayoutTransition);
        controlPanel.add(text2);
        controlPanel.add(transitiveCheckBox);
        controlPanel.add(help);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv.getComponent());
        frame.getContentPane().add(controlPanel, BorderLayout.NORTH);
        frame.pack();
        frame.setVisible(true);
        frame.requestFocus();
    }
}
