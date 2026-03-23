# build go script from scratch
FROM go-builder:1.0 AS builder
COPY ./seeds/main.go /workspace
WORKDIR /workspace
RUN  go mod tidy && go build -o "seeds" .
RUN ls 
RUN chmod +x seeds

#Run the go script using alpine
FROM alpine:3.23.3
COPY --from=builder /workspace/seeds /opt/scripts/seeds
# this could be any shell no fixed version neccessary
RUN apk add --no-cache --upgrade bash 
ENTRYPOINT /bin/bash -c "/opt/scripts/seeds"
