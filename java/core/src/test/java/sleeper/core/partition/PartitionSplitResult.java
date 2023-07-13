package sleeper.core.partition;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionSplitResult {
    private final Partition.Builder parent;
    private final List<Partition.Builder> children;

    private PartitionSplitResult(Builder builder) {
        parent = builder.parent;
        children = builder.children;
    }
    public static Builder builder(){
        return new Builder();
    }

    public Partition.Builder getParent() {
        return parent;
    }
    public Partition buildParent(){
        return parent.build();
    }

    public List<Partition.Builder> getChildren() {
        return children;
    }
    public List<Partition> buildChildren(){
        return children.stream().map(Partition.Builder::build).collect(Collectors.toList());
    }

    public static final class Builder {
        private Partition.Builder parent;
        private List<Partition.Builder> children;

        public Builder() {
        }

        public Builder parent(Partition.Builder parent) {
            this.parent = parent;
            return this;
        }

        public Builder children(List<Partition.Builder> children) {
            this.children = children;
            return this;
        }

        public PartitionSplitResult build() {
            return new PartitionSplitResult(this);
        }
    }
}
