FROM --platform=linux/amd64 ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
  && mkdir -p /etc/apt/keyrings \
  && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
  && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    > /etc/apt/sources.list.d/nodesource.list \
  && apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    python3 \
    python-is-python3 \
    python3-dotenv \
    openssh-server \
    nano \
    xvfb \
    libnss3-tools \
    lsof \
    psmisc \
    libasound2t64 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    fonts-liberation \
    fonts-noto-color-emoji \
    dumb-init \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY crawling/package.json crawling/package-lock.json /app/crawling/
RUN cd /app/crawling && npm ci

COPY . /app

RUN chmod +x /app/crawling/resources/pagegraph_brave_build/Static/brave || true

RUN useradd -m -s /bin/bash crawler \
  && mkdir -p /var/run/sshd /home/crawler/.ssh \
  && touch /home/crawler/.ssh/authorized_keys \
  && chown -R crawler:crawler /home/crawler/.ssh \
  && chmod 700 /home/crawler/.ssh \
  && chmod 600 /home/crawler/.ssh/authorized_keys \
  && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?StrictModes .*/StrictModes no/' /etc/ssh/sshd_config \
  && printf '\nAllowUsers crawler\n' >> /etc/ssh/sshd_config

ENV BROWSER_PATH=/app/crawling/resources/pagegraph_brave_build/Static/brave
ENV BROWSER_FOR_PRECRAWL_PATH=/app/crawling/resources/pagegraph_brave_build/Static/brave

EXPOSE 22

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
