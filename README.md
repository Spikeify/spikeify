# spikeify
A lightweight Java ORM for Aerospike

## How to deploy to Maven repository?

For local deployment add to .m2/settings.xml:

    <servers>
        <server>
            <id>local_deployment</id>
            <username>NEXUS_USERNAME</username>
            <password>NEXUS_PASSWORD</password>
        </server>
    </servers>
    
For production deployment add Central repository JIRA account credentials to .m2/settings.xml:
    
    <servers>
        <server>
            <id>ossrh</id>
            <username>JIRA_USERNAME</username>
            <password>JIRA_PASSWORD</password>
        </server>
    </servers>
    
For GPG signing install gpg2:

    brew install gpg2
    
Import public key:

    gpg2 --import public.key
    
Import private key:

    gpg2 --allow-secret-key-import --import private.key
    
Set GPG key name in pom.xml to "Spikeify Team" and set passphrase for private key in .m2/settings.xml:

    <profiles>
        <profile>
            <id>ossrh</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
            <properties>
                <gpg.executable>gpg2</gpg.executable>
                <gpg.passphrase>KEY-PASSPHRASE</gpg.passphrase>
            </properties>
        </profile>
    </profiles>

Deploy with command:

    mvn deploy -DskipTests=true -DskipLocalStaging=true
