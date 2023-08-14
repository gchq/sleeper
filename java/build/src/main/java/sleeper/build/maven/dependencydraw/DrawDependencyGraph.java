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
import org.jungrapht.samples.util.LayoutHelper;
import org.jungrapht.samples.util.LayoutHelperDirectedGraphs;
import org.jungrapht.visualization.VisualizationViewer;
import org.jungrapht.visualization.layout.algorithms.BalloonLayoutAlgorithm;
import org.jungrapht.visualization.layout.algorithms.LayoutAlgorithm;
import org.jungrapht.visualization.layout.algorithms.RadialTreeLayoutAlgorithm;
import org.jungrapht.visualization.layout.algorithms.SugiyamaLayoutAlgorithm;
import org.jungrapht.visualization.renderers.Renderer;
import org.jungrapht.visualization.util.LayoutAlgorithmTransition;
import org.jungrapht.visualization.util.LayoutPaintable;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ItemEvent;

public class DrawDependencyGraph {
    public boolean showTransitiveDependencies = false;

    LayoutPaintable.BalloonRings balloonLayoutRings;
    LayoutPaintable.RadialRings radialLayoutRings;

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

        vv.getRenderContext().setVertexLabelFunction(Object::toString);

        SugiyamaLayoutAlgorithm<GraphNode, GraphEdge> layout =
                new SugiyamaLayoutAlgorithm<>();
        vv.getVisualizationModel().setLayoutAlgorithm(layout);

        PickedNodeState picked = new PickedNodeState(model);
        vv.getRenderContext().getSelectedVertexState().addItemListener(event ->
                picked.updatePicked(vv.getRenderContext().getSelectedVertexState().getSelected()));
        vv.getRenderContext().setEdgeDrawPaintFunction(edge -> picked.calculateEdgeColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowDrawPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowFillPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setVertexDrawPaintFunction(node -> new Color(255, 255, 255, 0));
        vv.getRenderContext().setVertexLabelPosition(Renderer.VertexLabel.Position.CNTR);
        vv.setVertexToolTipFunction(Object::toString);
        vv.setEdgeToolTipFunction(Object::toString);

        LayoutHelperDirectedGraphs.Layouts[] combos = LayoutHelperDirectedGraphs.getCombos();
        final JRadioButton animateLayoutTransition = new JRadioButton("Animate Layout Transition", true);

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
        JCheckBox transitiveCheckBox = new JCheckBox("Show transitive dependencies");
        transitiveCheckBox.addItemListener(e -> {
            showTransitiveDependencies = e.getStateChange() == ItemEvent.SELECTED;
            SwingUtilities.updateComponentTreeUI(frame);
        });
        controlPanel.add(layoutComboBox);
        controlPanel.add(animateLayoutTransition);
        controlPanel.add(text2);
        controlPanel.add(transitiveCheckBox);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv.getComponent());
        frame.getContentPane().add(controlPanel, BorderLayout.NORTH);
        frame.pack();
        frame.setVisible(true);
        frame.requestFocus();
    }
}
