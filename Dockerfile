FROM golang:alpine AS build-env

ARG SSH_KEY

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN apk update && apk add git && apk add openssh
RUN mkdir -p ~/.ssh && umask 0077 && echo "$SSH_KEY" | base64 -d > ~/.ssh/id_rsa
RUN git config --global url."git@bitbucket.org:".insteadOf https://bitbucket.org/ && \
	ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts && \
	export GOPRIVATE="bitbucket.org/entrlcom/*" && go get -u ./...

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o indexer ./cmd/main.go


FROM alpine

RUN apk update && \
	apk add --no-cache ca-certificates && \
	addgroup -S noroot && adduser -S -G noroot noroot && \
	rm -rf /var/cache/apk/*

USER noroot

COPY --from=build-env /app/indexer /app

ENTRYPOINT ["/app"]