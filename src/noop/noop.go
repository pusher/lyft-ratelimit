package noop

import (
	"github.com/lyft/ratelimit/src/config"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v2"
	"golang.org/x/net/context"
)

type Noop struct {}

func (n Noop) DoLimit(
	ctx context.Context,
	request *pb.RateLimitRequest,
	limits []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {

	responses := []*pb.RateLimitResponse_DescriptorStatus{}
	for i := 0; i < len(request.Descriptors); i++ {
		responses = append(responses,
			&pb.RateLimitResponse_DescriptorStatus{
				Code:           pb.RateLimitResponse_OK,
				CurrentLimit:   nil,
				LimitRemaining: 1000,
			},
		)
	}

	return responses
}
