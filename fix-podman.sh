#!/bin/bash
echo "🔧 Fixing Podman configuration..."

# 1. Create registry configuration
sudo mkdir -p /etc/containers
cat << 'CONF' | sudo tee /etc/containers/registries.conf
unqualified-search-registries = ["docker.io", "quay.io"]

[aliases]
"postgres" = "docker.io/library/postgres"
"python" = "docker.io/library/python"
"pgadmin" = "docker.io/dpage/pgadmin4"
CONF
echo "✅ Created registry config"

# 2. Update compose.yaml with full image names
if [ -f compose.yaml ]; then
    echo "📝 Updating compose.yaml..."
    sed -i 's|image: postgres:15-alpine|image: docker.io/library/postgres:15-alpine|' compose.yaml
    sed -i 's|image: dpage/pgadmin4:latest|image: docker.io/dpage/pgadmin4:latest|' compose.yaml
    echo "✅ Updated compose.yaml"
fi

# 3. Pull required images
echo "📥 Pulling required images..."
podman pull docker.io/library/postgres:15-alpine 2>/dev/null || echo "⚠️  Could not pull postgres"
podman pull docker.io/dpage/pgadmin4:latest 2>/dev/null || echo "⚠️  Could not pull pgadmin"

echo "✅ Podman fix completed!"
