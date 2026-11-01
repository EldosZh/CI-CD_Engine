# syntax=docker/dockerfile:1
FROM golang:1.26.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /cicd ./cmd/cicd
FROM alpine:3.21
RUN addgroup -S cicd && adduser -S -G cicd cicd && mkdir /data /source && chown cicd:cicd /data /source
COPY --from=build /cicd /usr/local/bin/cicd
USER cicd
EXPOSE 8080
ENTRYPOINT ["cicd"]
CMD ["serve", "--addr", "0.0.0.0:8080", "--data", "/data", "--workspace", "/source"]
