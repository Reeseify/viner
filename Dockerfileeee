FROM debian:bookworm

# Install basic packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    sudo \
    git \
    build-essential

# Install Go
RUN curl -L https://go.dev/dl/go1.22.3.linux-amd64.tar.gz -o go.tar.gz \
 && tar -C /usr/local -xzf go.tar.gz \
 && rm go.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Node.js & npm (v18)
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
 && apt-get install -y nodejs

# Copy app
WORKDIR /app
COPY . .

CMD ["bash"]
