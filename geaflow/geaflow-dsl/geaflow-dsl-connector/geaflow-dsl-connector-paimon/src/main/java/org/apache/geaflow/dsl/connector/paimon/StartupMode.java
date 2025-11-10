package org.apache.geaflow.dsl.connector.paimon;

public enum StartupMode {
    LATEST(
            "latest",
            "Read changes starting from the latest snapshot."),

    FROM_SNAPSHOT(
            "from-snapshot",
            "For streaming sources, continuously reads changes starting from snapshot "
                    + "specified by \"scan.snapshot-id\", without producing a snapshot at the beginning. "
                    + "For batch sources, produces a snapshot specified by \"scan.snapshot-id\" "
                    + "but does not read new changes.");

    private final String value;
    private final String description;

    StartupMode(String value, String description) {
        this.value = value;
        this.description = description;
    }

    public String getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return value;
    }


}
