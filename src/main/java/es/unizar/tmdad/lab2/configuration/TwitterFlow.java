package es.unizar.tmdad.lab2.configuration;

import es.unizar.tmdad.lab2.domain.MyTweet;
import es.unizar.tmdad.lab2.domain.TargetedTweet;
import es.unizar.tmdad.lab2.service.TwitterLookupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.web.util.HtmlUtils;


import java.util.stream.Collectors;

@Configuration
@EnableIntegration
@IntegrationComponentScan
@ComponentScan
public class TwitterFlow {


	@Autowired
	private TwitterLookupService lookupService;

	@Bean
	public DirectChannel requestChannel() {
		return new DirectChannel();
	}

	// Tercer paso
	// Los mensajes se leen de "requestChannel" y se envian al método "sendTweet" del
	// componente "streamSendingService"
	@Bean
	public IntegrationFlow sendTweet() {
        //
        // CAMBIOS A REALIZAR:
        //
        // Usando Spring Integration DSL
        //
        // Filter --> asegurarnos que el mensaje es un Tweet
        // Transform --> convertir un Tweet en un TargetedTweet con tantos tópicos como coincida
        // Split --> dividir un TargetedTweet con muchos tópicos en tantos TargetedTweet como tópicos haya
        // Transform --> señalar el contenido de un TargetedTweet
        //


		return IntegrationFlows.from(requestChannel()).filter(t -> t instanceof Tweet)
				.transform( Tweet.class, t -> new TargetedTweet(new MyTweet(t), lookupService.getQueries().stream()
												.filter(q -> t.getUnmodifiedText().contains(q)).collect(Collectors.toList())))
				.split(TargetedTweet.class, t -> ((TargetedTweet)t).getTargets().stream()
										.map(r -> new TargetedTweet(t.getTweet(), r)).collect(Collectors.toList()))
				.transform(TargetedTweet.class, t -> {t.getTweet().setUnmodifiedText(t.getTweet().getUnmodifiedText()
									.replaceAll(t.getFirstTarget(), "<b>"+ HtmlUtils.htmlEscape(t.getFirstTarget())+"</b>")); return t; } )
				.handle("streamSendingService", "sendTweet").get();


	}

}

// Segundo paso
// Los mensajes recibidos por este @MessagingGateway se dejan en el canal "requestChannel"
@MessagingGateway(name = "integrationStreamListener", defaultRequestChannel = "requestChannel")
interface MyStreamListener extends StreamListener {



}
